package kayak

import (
	"sync"

	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

// the log syncer should be rebuilt every time a leader comes to power.
type logSyncer struct {

	// the primary replica instance. ()
	self *replica

	// the current term (extracted as the term can be changed by the leader machine)
	term term

	// the logger (injected by parent.  do not use root's logger)
	logger common.Logger

	// used to determine peer sync state
	syncers map[uuid.UUID]*peerSyncer

	// Used to access/update peer states.
	syncersLock sync.Mutex

	// used to indicate whether a catastrophic failure has occurred
	failure error

	// the closing channel.  Independent of leader.
	closed chan struct{}
	closer chan struct{}
}

func newLogSyncer(logger common.Logger, self *replica) *logSyncer {
	s := &logSyncer{
		self:    self,
		term:    self.CurrentTerm(),
		logger:  logger,
		syncers: make(map[uuid.UUID]*peerSyncer),
		closed:  make(chan struct{}),
		closer:  make(chan struct{}, 1),
	}

	s.start()
	return s
}

func (l *logSyncer) Closed() bool {
	select {
	default:
		return false
	case <-l.closed:
		return true
	}
}

func (l *logSyncer) Close() error {
	return l.shutdown(nil)
}

func (l *logSyncer) shutdown(err error) error {
	select {
	case <-l.closed:
		return l.failure
	case l.closer <- struct{}{}:
	}

	l.failure = err
	close(l.closed)
	return err
}

func (s *logSyncer) handleRosterChange(peers []peer) {
	cur, active := s.Syncers(), make(map[uuid.UUID]*peerSyncer)

	// Add any missing
	for _, p := range peers {
		if p.Id == s.self.Id {
			continue
		}

		if sync, ok := cur[p.Id]; ok {
			active[p.Id] = sync
		} else {
			active[p.Id] = newPeerSyncer(s.logger, s.self, s.term, p)
		}
	}

	// Remove any missing
	for id, sync := range cur {
		if _, ok := active[id]; !ok {
			sync.shutdown(nil)
		}
	}

	s.SetSyncers(active)
}

func (s *logSyncer) start() {
	peers, ver := s.self.Roster.Get()
	s.handleRosterChange(peers)

	var ok bool
	go func() {
		for {
			peers, ver, ok = s.self.Roster.Wait(ver)
			if s.Closed() || !ok {
				return
			}

			s.handleRosterChange(peers)
		}
	}()
}

func (s *logSyncer) Append(event Event, source uuid.UUID, seq int, kind int) (item LogItem, err error) {
	committed := make(chan struct{}, 1)
	go func() {
		// append
		item, err = s.self.Log.Append(event, s.term.Num, source, seq, kind)
		if err != nil {
			s.shutdown(err)
			return
		}

		// wait for majority.
		majority := s.self.Majority() - 1
		for done := make(map[uuid.UUID]struct{}); len(done) < majority; {
			for _, p := range s.self.Others() {
				if _, ok := done[p.Id]; ok {
					continue
				}

				syncer := s.Syncer(p.Id)
				if syncer == nil {
					continue
				}

				index, term := syncer.GetPrevIndexAndTerm()
				if index >= item.Index && term == s.term.Num {
					done[p.Id] = struct{}{}
				}
			}

			if s.Closed() {
				return
			}
		}

		s.self.Log.Commit(item.Index) // commutative, so safe in the event of out of order commits.
		committed <- struct{}{}
	}()

	select {
	case <-s.closed:
		return LogItem{}, common.Or(s.failure, ClosedError)
	case <-committed:
		return item, nil
	}
}

func (s *logSyncer) Syncer(id uuid.UUID) *peerSyncer {
	s.syncersLock.Lock()
	defer s.syncersLock.Unlock()
	return s.syncers[id]
}

func (s *logSyncer) Syncers() map[uuid.UUID]*peerSyncer {
	s.syncersLock.Lock()
	defer s.syncersLock.Unlock()
	ret := make(map[uuid.UUID]*peerSyncer)
	for k, v := range s.syncers {
		ret[k] = v
	}
	return ret
}

func (s *logSyncer) SetSyncers(syncers map[uuid.UUID]*peerSyncer) {
	s.syncersLock.Lock()
	defer s.syncersLock.Unlock()
	s.syncers = syncers
}

//
type peerSyncer struct {
	logger common.Logger

	self *replica
	peer peer
	term term

	log *eventLog

	prevIndex int
	prevTerm  int
	prevLock  *sync.RWMutex

	failure error
	closed  chan struct{}
	closer  chan struct{}
}

func newPeerSyncer(logger common.Logger, self *replica, term term, peer peer) *peerSyncer {
	sync := &peerSyncer{
		logger:    logger.Fmt("Sync(%v)", peer),
		self:      self,
		peer:      peer,
		term:      term,
		log:       self.Log,
		prevIndex: -1,
		prevTerm:  -1,
		closed:    make(chan struct{}),
		closer:    make(chan struct{}, 1),
	}
	sync.start()
	return sync
}

func (l *peerSyncer) shutdown(err error) error {
	select {
	case <-l.closed:
		return l.failure
	case l.closer <- struct{}{}:
	}

	l.failure = err
	close(l.closed)
	return err
}

func (l *peerSyncer) Closed() bool {
	select {
	default:
		return false
	case <-l.closed:
		return true
	}
}

func (l *peerSyncer) GetPrevIndexAndTerm() (int, int) {
	l.prevLock.RLock()
	defer l.prevLock.RUnlock()
	return l.prevIndex, l.prevTerm
}

func (l *peerSyncer) SetPrevIndexAndTerm(index int, term int) {
	l.prevLock.Lock()
	defer l.prevLock.Unlock()
	l.prevIndex = index
	l.prevTerm = term
}

// Per raft: A leader never overwrites or deletes entries in its log; it only appends new entries. §3.5
// no need to worry about truncations here...however, we do need to worry about compactions interrupting
// syncing.
func (s *peerSyncer) start() {
	go func() {
		defer s.logger.Info("Shutting down")

		var cl *client
		defer func() {
			if cl != nil {
				cl.Close()
			}
		}()

		// Start syncing at last index
		prevIndex, prevTerm, err := s.log.Last()
		if err != nil {
			s.shutdown(err)
			return
		}

		for {
			next, ok := s.log.head.WaitForChange(prevIndex + 1)
			if !ok || s.Closed() {
				return
			}

			// loop until this peer is completely caught up to head!
			for prevIndex < next {
				if s.Closed() {
					return
				}

				s.logger.Debug("Currently (%v/%v)", prevIndex, next)

				// might have to reinitialize client after each batch.
				if cl == nil {
					cl, err = s.peer.Connect(s.self.Ctx, s.closed)
					if err != nil {
						return
					}
				}

				// scan a full batch of events.
				batch, err := s.log.Scan(prevIndex+1, prevIndex+1+256)
				if err != nil {
					s.shutdown(err)
					return
				}

				// send the append request.
				resp, err := cl.Replicate(s.self.Id, s.term.Num, prevIndex, prevTerm, batch, s.log.Committed())
				if err != nil {
					s.logger.Error("Unable to append events [%v]", err)
					cl = nil
					continue
				}

				// make sure we're still a leader.
				if resp.term > s.term.Num {
					s.logger.Error("No longer leader.")
					s.shutdown(NotLeaderError)
					return
				}

				// if it was successful, progress the peer's index and term
				if resp.success {
					prev := batch[len(batch)-1]
					prevIndex, prevTerm = prev.Index, prevTerm
					s.SetPrevIndexAndTerm(prevIndex, prevTerm)
					continue
				}

				// consistency check failed, start moving backwards one index at a time.
				// TODO: Install snapshot

				prev, ok, err := s.log.Get(prevIndex - 1)
				if err != nil {
					s.shutdown(err)
					return
				}

				if ok {
					prevIndex, prevTerm = prev.Index, prevTerm
					s.SetPrevIndexAndTerm(prevIndex, prevTerm)
					continue
				}

				// INSTALL SNAPSHOT
				s.SetPrevIndexAndTerm(-1, -1)
			}

			s.logger.Debug("Sync'ed to [%v]", next)
		}
	}()
}

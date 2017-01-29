package kayak

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

// the log syncer should be rebuilt every time a leader comes to power.
type logSyncer struct {
	// the context (injected by parent and spawned)
	ctx common.Context

	// the logger (injected by parent.  do not use root's logger)
	logger common.Logger

	// the core syncer lifecycle
	ctrl common.Control

	// the primary replica instance. ()
	self *replica

	// the current term (extracted as the term can be changed by the leader machine)
	term term

	// used to determine peer sync state
	syncers map[uuid.UUID]*peerSyncer

	// Used to access/update peer states.
	syncersLock sync.Mutex
}

func newLogSyncer(ctx common.Context, self *replica) *logSyncer {
	ctx = ctx.Sub("Syncer")

	s := &logSyncer{
		ctx:     ctx,
		logger:  ctx.Logger(),
		ctrl:    ctx.Control(),
		self:    self,
		term:    self.CurrentTerm(),
		syncers: make(map[uuid.UUID]*peerSyncer),
	}

	s.start()
	return s
}

func (l *logSyncer) Close() error {
	l.ctrl.Close()
	return nil
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
			active[p.Id] = newPeerSyncer(s.ctx, s.self, s.term, p)
		}
	}

	// Remove any missing
	for id, sync := range cur {
		if _, ok := active[id]; !ok {
			sync.control.Close()
		}
	}

	s.logger.Info("Setting roster: %v", active)
	s.SetSyncers(active)
}

func (s *logSyncer) start() {
	peers, ver := s.self.Roster.Get()
	s.handleRosterChange(peers)

	var ok bool
	go func() {
		for {
			peers, ver, ok = s.self.Roster.Wait(ver)
			if s.ctrl.IsClosed() || !ok {
				return
			}
			s.handleRosterChange(peers)
		}
	}()
}

func (s *logSyncer) Append(append appendEvent) (item LogItem, err error) {
	committed := make(chan struct{}, 1)
	go func() {
		// append
		item, err = s.self.Log.Append(append.Event, s.term.Num, append.Source, append.Seq, append.Kind)
		if err != nil {
			s.ctrl.Fail(err)
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

			if s.ctrl.IsClosed() {
				return
			}
		}

		s.self.Log.Commit(item.Index) // commutative, so safe in the event of out of order commits.
		committed <- struct{}{}
	}()

	select {
	case <-s.ctrl.Closed():
		return LogItem{}, common.Or(s.ctrl.Failure(), ClosedError)
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
	logger    common.Logger
	control   common.Control
	peer      peer
	term      term
	self      *replica
	prevIndex int
	prevTerm  int
	prevLock  sync.RWMutex
}

func newPeerSyncer(ctx common.Context, self *replica, term term, peer peer) *peerSyncer {
	sub := ctx.Sub("Sync(%v)", peer)
	go func() {
		select {
		case <-sub.Control().Closed():
			ctx.Control().Fail(sub.Control().Failure())
		case <-ctx.Control().Closed():
			return
		}
	}()

	sync := &peerSyncer{
		logger:    sub.Logger(),
		control:   sub.Control(),
		self:      self,
		peer:      peer,
		term:      term,
		prevIndex: -1,
		prevTerm:  -1,
	}
	sync.start()
	return sync
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

func (l *peerSyncer) InstallSnapshot(cl *rpcClient, snapshot StoredSnapshot) (bool, error) {
	size := snapshot.Size()
	for i := 0; i < size; {
		if l.control.IsClosed() {
			return false, ClosedError
		}

		beg, end := i, common.Min(size-1, i+255)

		l.logger.Info("Sending snapshot segment [%v,%v]", beg, end)
		batch, err := snapshot.Scan(beg, end)
		if err != nil {
			return false, errors.Wrapf(err, "Error scanning batch [%v, %v]", beg, end)
		}

		segment := installSnapshot{
			l.self.Id,
			l.term.Num,
			snapshot.Config(),
			size,
			snapshot.LastIndex(),
			snapshot.LastTerm(),
			beg,
			batch}

		resp, err := cl.InstallSnapshot(segment)
		if err != nil {
			return false, errors.Wrapf(err, "Error sending snapshot segment: %v", segment)
		}

		if resp.term > l.term.Num {
			return false, NotLeaderError
		}

		if !resp.success {
			return false, nil
		}

		i += len(batch)
	}

	return true, nil
}

// Per raft: A leader never overwrites or deletes entries in its log; it only appends new entries. §3.5
// no need to worry about truncations here...however, we do need to worry about compactions interrupting
// syncing.
func (s *peerSyncer) start() {
	s.logger.Info("Starting")
	go func() {
		defer s.control.Close()
		defer s.logger.Info("Shutting down")

		var cl *rpcClient
		defer func() {
			if cl != nil {
				cl.Close()
			}
		}()

		// Start syncing at last index
		lastIndex, lastTerm, err := s.self.Log.Last()
		if err != nil {
			s.control.Fail(err)
			return
		}

		prev, ok, err := s.self.Log.Get(lastIndex - 1)
		if err != nil {
			s.control.Fail(err)
			return
		}

		if !ok {
			prev = LogItem{Index: lastIndex, Term: lastTerm}
		}

		for {
			next, ok := s.self.Log.head.WaitUntil(prev.Index + 1)
			if !ok || s.control.IsClosed() {
				return
			}

			// loop until this peer is completely caught up to head!
			for prev.Index < next {
				if s.control.IsClosed() {
					return
				}

				s.logger.Debug("Position [%v/%v]", prev.Index, next)

				// might have to reinitialize client after each batch.
				if cl == nil {
					cl, err = s.peer.Connect(s.self.Ctx, s.control.Closed())
					if err != nil {
						s.control.Fail(err)
						return
					}
				}

				// scan a full batch of events.
				batch, err := s.self.Log.Scan(prev.Index+1, prev.Index+1+256)
				if err != nil {
					s.logger.Error("Error scanning batch: %v", batch)
					s.control.Fail(err)
					return
				}

				// s.logger.Debug("Sending batch (%v): %v", prev.Index+1, len(batch))

				// send the append request.
				resp, err := cl.Replicate(newReplication(s.self.Id, s.term.Num, prev.Index, prev.Term, batch, s.self.Log.Committed()))
				if err != nil {
					s.logger.Error("Unable to append events [%v]", err)
					cl = nil
					continue
				}

				// make sure we're still a leader.
				if resp.term > s.term.Num {
					s.control.Fail(NotLeaderError)
					return
				}

				// if it was successful, progress the peer's index and term
				if resp.success {
					prev = batch[len(batch)-1]
					s.SetPrevIndexAndTerm(prev.Index, prev.Term)
					continue
				}

				s.logger.Error("Consistency check failed. Received hint [%v]", resp.hint)
				prev, ok, err = s.self.Log.Get(common.Min(resp.hint, prev.Index-1))
				if err != nil {
					s.control.Fail(err)
					return
				}

				if ok {
					s.logger.Error("Consistency check failed. Moved to [%v]", prev.Index)
					s.SetPrevIndexAndTerm(prev.Index, prev.Term)
					continue
				}

				s.logger.Info("Too far behind. Installing snapshot.")

				// Okay, we're now into snapshot territory
				snapshot, err := s.self.Log.Snapshot()
				if err != nil {
					s.control.Fail(err)
					return
				}

				ok, err := s.InstallSnapshot(cl, snapshot)
				if err != nil {
					s.logger.Error("Error: %v", err)
					s.control.Fail(err)
					return
				}

				if ok {
					prev = LogItem{Index: snapshot.LastIndex(), Term: snapshot.LastTerm()}
					continue
				}

				lastIndex, lastTerm, err := s.self.Log.Last()
				if err != nil {
					s.control.Fail(err)
					return
				}

				prev = LogItem{Index: lastIndex, Term: lastTerm}
			}

			s.logger.Debug("Sync'ed to [%v]", prev.Index)
		}
	}()
}

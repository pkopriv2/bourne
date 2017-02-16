package kayak

import (
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

// TODO: Start using connection pools instead of initializing clients manually.

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

	// the current term (extracted because the sync'er needs consistent view of term)
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

func (s *logSyncer) spawnSyncer(p Peer) *peerSyncer {
	sync := newPeerSyncer(s.ctx, s.self, s.term, p)
	go func() {
		select {
		case <-sync.ctrl.Closed():
			s.logger.Error("Syncer died: %+v", sync.ctrl.Failure())
			if cause := common.Extract(sync.ctrl.Failure(), NotLeaderError); cause == nil || cause == NotLeaderError {
				s.logger.Info("Shutting down log sync'er")
				s.ctrl.Fail(sync.ctrl.Failure())
				return
			}

			s.logger.Info("Restarting sync'er: %v", p)
			s.handleRosterChange(s.self.Cluster())
			return
		case <-s.ctrl.Closed():
			return
		}
	}()
	return sync
}

func (s *logSyncer) handleRosterChange(peers []Peer) {
	cur, active := s.Syncers(), make(map[uuid.UUID]*peerSyncer)

	// Add any missing
	for _, p := range peers {
		if p.Id == s.self.Self.Id {
			continue
		}

		if sync, ok := cur[p.Id]; ok && !sync.ctrl.IsClosed() {
			active[p.Id] = sync
			continue
		}

		active[p.Id] = s.spawnSyncer(p)
	}

	// Remove any missing
	for id, sync := range cur {
		if _, ok := active[id]; !ok {
			sync.ctrl.Close()
		}
	}

	after := make([]Peer, 0, len(active))
	for _, syncer := range active {
		after = append(after, syncer.peer)
	}

	s.logger.Info("Setting roster: %v", after)
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

func (s *logSyncer) Append(append appendEvent) (item Entry, err error) {
	committed := make(chan struct{}, 1)
	go func() {
		// append
		item, err = s.self.Log.Append(append.Event, s.term.Num, append.Kind)
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
		return Entry{}, common.Or(s.ctrl.Failure(), ClosedError)
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

// a peer syncer is responsible for sync'ing a single peer.
type peerSyncer struct {
	logger    common.Logger
	ctrl      common.Control
	peer      Peer
	term      term
	self      *replica
	prevIndex int
	prevTerm  int
	prevLock  sync.RWMutex
	pool      common.ObjectPool // *rpcClient
}

func newPeerSyncer(ctx common.Context, self *replica, term term, peer Peer) *peerSyncer {
	sub := ctx.Sub("Sync(%v)", peer)
	sync := &peerSyncer{
		logger:    sub.Logger(),
		ctrl:      sub.Control(),
		self:      self,
		peer:      peer,
		term:      term,
		prevIndex: -1,
		prevTerm:  -1,
		pool:      peer.ClientPool(ctx, self.Network, 30*time.Second, 2),
	}
	sync.start()
	return sync
}

func (s *peerSyncer) Close() error {
	return s.ctrl.Close()
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

func (s *peerSyncer) send(cancel <-chan struct{}, fn func(cl *rpcClient) error) error {
	raw := s.pool.TakeOrCancel(cancel)
	if raw == nil {
		return errors.WithStack(common.CanceledError)
	}

	if err := fn(raw.(*rpcClient)); err != nil {
		s.pool.Fail(raw)
		return errors.WithStack(err)
	} else {
		s.pool.Return(raw)
		return nil
	}
}

func (s *peerSyncer) heartbeat(cancel <-chan struct{}) (resp response, err error) {
	err = s.send(cancel, func(cl *rpcClient) error {
		resp, err = cl.Replicate(newHeartBeat(s.self.Id, s.term.Num, s.self.Log.Committed()))
		return err
	})
	return

}

// Per raft: A leader never overwrites or deletes entries in its log; it only appends new entries. §3.5
// no need to worry about truncations here...however, we do need to worry about compactions interrupting
// syncing.
func (s *peerSyncer) start() {
	s.logger.Info("Starting")
	go func() {
		defer s.ctrl.Close()
		defer s.logger.Info("Shutting down")

		var cl *rpcClient
		defer func() {
			if cl != nil {
				cl.Close()
			}
		}()

		prev, err := s.syncInit()
		if err != nil {
			s.ctrl.Fail(err)
			return
		}

		for {
			next, ok := s.self.Log.head.WaitUntil(prev.Index + 1)
			if !ok || s.ctrl.IsClosed() {
				return
			}

			// loop until this peer is completely caught up to head!
			for prev.Index < next {
				if s.ctrl.IsClosed() {
					return
				}

				// might have to reinitialize client after each batch.
				s.logger.Debug("Position [%v/%v]", prev.Index, next)
				if cl == nil {
					s.logger.Debug("Re-initializing client")
					cl, err = s.peer.Client(s.self.Ctx, s.self.Network, s.self.ConnTimeout)
					if err != nil {
						s.ctrl.Fail(err)
						return
					}
				}

				// send the batch
				cl, prev, ok, err = s.sendBatch(cl, prev, next)
				if err != nil {
					s.ctrl.Fail(err)
					return
				}

				// any network errors, start this iteration over
				if cl == nil {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// if everything was ok, advance the index and term
				if ok {
					s.SetPrevIndexAndTerm(prev.Index, prev.Term)
					continue
				}

				s.logger.Info("Too far behind [%v,%v]. Installing snapshot.", prev.Index, prev.Term)
				cl, prev, err = s.installSnapshot(cl)
				if err != nil {
					s.ctrl.Fail(err)
					return
				}

				// any network errors, start this iteration over
				if cl == nil {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				s.SetPrevIndexAndTerm(prev.Index, prev.Term)
			}

			s.logger.Debug("Sync'ed to [%v]", prev.Index)
		}
	}()
}

func (s *peerSyncer) score(cancel <-chan struct{}) (int, error) {

	// delta just calulcates distance from sync position to max
	delta := func() (int, error) {
		if s.ctrl.IsClosed() {
			return 0, common.Or(ClosedError, s.ctrl.Failure())
		}

		max, _, err := s.self.Log.Last()
		if err != nil {
			return 0, err
		}

		idx, _ := s.GetPrevIndexAndTerm()
		return max - idx, nil
	}

	// watch the sync'er.
	prevDelta := math.MaxInt32

	score := 0
	for rounds := 0; rounds < 30; rounds++ {
		s.heartbeat(cancel)

		curDelta, err := delta()
		if err != nil {
			return 0, err
		}

		// This is totally arbitrary.
		if curDelta < 2 && score >= 1 {
			break
		}

		if curDelta < 8 && score >= 3 {
			break
		}

		if curDelta < 128 && score >= 4 {
			break
		}

		if curDelta < 1024 && score >= 5 {
			break
		}

		if curDelta <= prevDelta {
			score++
		} else {
			score--
		}

		s.logger.Info("Delta [%v] after [%v] rounds.  Score: [%v]", curDelta, rounds+1, score)
		time.Sleep(s.self.ElectionTimeout / 5)
		prevDelta = curDelta
	}

	return score, nil
}

// returns the starting position for syncing a newly initialized sync'er
func (s *peerSyncer) syncInit() (Entry, error) {
	lastIndex, lastTerm, err := s.self.Log.Last()
	if err != nil {
		return Entry{}, err
	}

	prev, ok, err := s.self.Log.Get(lastIndex - 1)
	if ok || err != nil {
		return prev, err
	}

	return Entry{Index: lastIndex, Term: lastTerm}, nil
}

// Sends a batch up to the horizon
func (s *peerSyncer) sendBatch(cl *rpcClient, prev Entry, horizon int) (*rpcClient, Entry, bool, error) {
	// scan a full batch of events.
	batch, err := s.self.Log.Scan(prev.Index+1, common.Min(horizon, prev.Index+1+256))
	if err != nil {
		return cl, prev, false, err
	}

	// send the append request.
	resp, err := cl.Replicate(newReplication(s.self.Id, s.term.Num, prev.Index, prev.Term, batch, s.self.Log.Committed()))
	if err != nil {
		s.logger.Error("Unable to replicate batch [%v]", err)
		return nil, prev, false, err
	}

	// make sure we're still a leader.
	if resp.term > s.term.Num {
		return cl, prev, false, NotLeaderError
	}

	// if it was successful, progress the peer's index and term
	if resp.success {
		return cl, batch[len(batch)-1], true, nil
	}

	s.logger.Error("Consistency check failed. Received hint [%v]", resp.hint)
	prev, ok, err := s.self.Log.Get(common.Min(resp.hint, prev.Index-1))
	return cl, prev, ok, err
}

func (s *peerSyncer) installSnapshot(cl *rpcClient) (*rpcClient, Entry, error) {
	snapshot, err := s.self.Log.Snapshot()
	if err != nil {
		return cl, Entry{}, err
	}

	cl, err = s.sendSnapshot(cl, snapshot)
	if err != nil {
		return cl, Entry{}, err
	}
	if cl != nil {
		return cl, Entry{Index: snapshot.LastIndex(), Term: snapshot.LastTerm()}, nil
	}

	prev, err := s.syncInit()
	return cl, prev, err
}

// sends the snapshot to the client
func (l *peerSyncer) sendSnapshot(cl *rpcClient, snapshot StoredSnapshot) (*rpcClient, error) {
	size := snapshot.Size()
	sendSegment := func(cl *rpcClient, offset int, batch []Event) (*rpcClient, error) {
		segment := installSnapshot{
			l.self.Id,
			l.term.Num,
			snapshot.Config(),
			size,
			snapshot.LastIndex(),
			snapshot.LastTerm(),
			offset,
			batch}

		resp, err := cl.InstallSnapshot(segment)
		if err != nil {
			return nil, err
		}

		if resp.term > l.term.Num || !resp.success {
			return cl, NotLeaderError
		}

		if !resp.success {
			return cl, NotLeaderError
		}

		return cl, nil
	}

	if size == 0 {
		return sendSegment(cl, 0, []Event{})
	}

	for i := 0; i < size; {
		if l.ctrl.IsClosed() {
			return cl, ClosedError
		}

		beg, end := i, common.Min(size-1, i+255)

		l.logger.Info("Sending snapshot segment [%v,%v]", beg, end)
		batch, err := snapshot.Scan(beg, end)
		if err != nil {
			return cl, errors.Wrapf(err, "Error scanning batch [%v, %v]", beg, end)
		}

		cl, err = sendSegment(cl, beg, batch)
		if cl == nil || err != nil {
			return cl, err
		}

		i += len(batch)
	}

	return cl, nil
}

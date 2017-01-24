package kayak

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	uuid "github.com/satori/go.uuid"
)

type leaderSpawner struct {
	ctx      common.Context
	in       chan *replica
	follower chan<- *replica
	closed   chan struct{}
}

func newLeaderSpawner(ctx common.Context, in chan *replica, follower chan<- *replica, closed chan struct{}) *leaderSpawner {
	ret := &leaderSpawner{ctx, in, follower, closed}
	ret.start()
	return ret
}

func (c *leaderSpawner) start() {
	go func() {
		for {
			select {
			case <-c.closed:
				return
			case i := <-c.in:
				spawnLeader(c.follower, i)
			}
		}
	}()
}

type leader struct {
	logger     common.Logger
	follower   chan<- *replica
	syncer     *logSyncer
	proxyPool  concurrent.WorkPool
	appendPool concurrent.WorkPool
	term       term
	replica    *replica
	closed     chan struct{}
	closer     chan struct{}
}

func spawnLeader(follower chan<- *replica, replica *replica) {
	replica.Term(replica.CurrentTerm().Num, &replica.Id, &replica.Id)

	logger := replica.Logger.Fmt("Leader(%v)", replica.CurrentTerm())
	logger.Info("Becoming leader")

	// syncer := newLogSyncer(replica, logger)

	closed := make(chan struct{})
	l := &leader{
		logger:     logger,
		follower:   follower,
		// syncer:     syncer,
		proxyPool:  concurrent.NewWorkPool(10),
		appendPool: concurrent.NewWorkPool(10),
		term:       replica.CurrentTerm(),
		replica:    replica,
		closed:     closed,
		closer:     make(chan struct{}, 1),
	}

	l.start()
}

func (l *leader) transition(ch chan<- *replica) {
	select {
	case <-l.closed:
	case ch <- l.replica:
	}

	l.Close()
}

func (l *leader) Close() error {
	select {
	case <-l.closed:
		return ClosedError
	case l.closer <- struct{}{}:
	}

	l.proxyPool.Close()
	l.appendPool.Close()
	l.syncer.Close()
	close(l.closed)
	return nil
}

func (l *leader) start() {
	// Establish leadership
	l.broadcastHeartbeat()

	// Proxy routine.
	go func() {
		for {
			select {
			case <-l.replica.closed:
				return
			case <-l.closed:
				return
			case append := <-l.replica.RemoteAppends:
				l.handleRemoteAppend(append)
			}
		}
	}()

	// Main routine
	go func() {
		for {
			timer := time.NewTimer(l.replica.ElectionTimeout / 5)
			l.logger.Debug("Resetting timeout [%v]", l.replica.ElectionTimeout/5)

			select {
			case <-l.closed:
				return
			case <-l.replica.closed:
				l.Close()
				return
			case append := <-l.replica.LocalAppends:
				l.handleLocalAppend(append)
			case snapshot := <-l.replica.Snapshots:
				l.handleInstallSnapshot(snapshot)
			case events := <-l.replica.Replications:
				l.handleReplication(events)
			case ballot := <-l.replica.VoteRequests:
				l.handleRequestVote(ballot)
			case <-timer.C:
				l.broadcastHeartbeat()
			case <-l.syncer.closed:
				l.logger.Error("Sync'er closed: %v", l.syncer.failure)
				if l.syncer.failure == NotLeaderError {
					l.transition(l.follower)
					return
				}
				return
			}
		}
	}()
}

func (c *leader) handleRemoteAppend(append machineAppend) {
	err := c.proxyPool.Submit(func() {
		append.Return(c.replica.LocalAppend(append.Event, append.Source, append.Seq, append.Kind))
	})

	if err != nil {
		append.Fail(errors.Wrapf(err, "Error submitting work to proxy pool."))
	}
}

func (c *leader) handleLocalAppend(append machineAppend) {
	err := c.appendPool.SubmitTimeout(1000*time.Millisecond, func() {
		append.Return(c.syncer.Append(append.Event, append.Source, append.Seq, append.Kind))
	})

	if err != nil {
		append.Fail(errors.Wrapf(err, "Error submitting work to append pool."))
	}
}

func (c *leader) handleInstallSnapshot(snapshot installSnapshot) {
	snapshot.Reply(c.term.Num, false)
}

func (c *leader) handleRequestVote(vote requestVote) {
	c.logger.Debug("Handling request vote: %v", vote)

	// handle: previous or current term vote.  (immediately decline.  already leader)
	if vote.term <= c.term.Num {
		vote.reply(c.term.Num, false)
		return
	}

	// handle: future term vote.  (move to new term.  only accept if candidate log is long enough)
	max, err := c.replica.Log.Max()
	if err != nil {
		vote.reply(vote.term, false)
		return
	}

	if vote.maxLogIndex >= max.Index && vote.maxLogTerm >= max.Term {
		c.replica.Term(vote.term, nil, &vote.id)
		vote.reply(vote.term, true)
	} else {
		c.replica.Term(vote.term, nil, nil)
		vote.reply(vote.term, false)
	}

	c.transition(c.follower)
}

func (c *leader) handleReplication(append replicateEvents) {
	if append.term <= c.term.Num {
		append.reply(c.term.Num, false)
		return
	}

	c.replica.Term(append.term, &append.id, &append.id)
	append.reply(append.term, false)
	c.transition(c.follower)
}

func (c *leader) broadcastHeartbeat() {
	ch := c.replica.Broadcast(func(cl *client) response {
		resp, err := cl.Replicate(c.replica.Id, c.term.Num, -1, -1, []LogItem{}, c.replica.Log.Committed())
		if err != nil {
			return response{c.term.Num, false}
		} else {
			return resp
		}
	})

	timer := time.NewTimer(c.replica.ElectionTimeout)
	for i := 0; i < c.replica.Majority()-1; {
		select {
		case <-c.closed:
			return
		case resp := <-ch:
			if resp.term > c.term.Num {
				c.replica.Term(resp.term, nil, c.term.VotedFor)
				c.transition(c.follower)
				return
			}

			i++
		case <-timer.C:
			c.replica.Term(c.term.Num, nil, c.term.VotedFor)
			c.transition(c.follower)
			return
		}
	}
}

// the log syncer should be rebuilt every time a leader comes to power.
type logSyncer struct {

	// the primary member instance. (guaranteed to be immutable)
	root *replica

	// the logger (injected by parent.  do not use root's logger)
	logger common.Logger

	// tracks index of last consumer item
	prevIndices map[uuid.UUID]int

	// tracks term of last consumer item
	prevTerms map[uuid.UUID]int

	// Used to access/update peer states.
	prevLock sync.Mutex

	// used to indicate whether a catastrophic failure has occurred
	failure error

	// the closing channel.  Independent of leader.
	closed chan struct{}
	closer chan struct{}
}

func newLogSyncer(inst *replica, logger common.Logger) *logSyncer {
	s := &logSyncer{
		root:        inst,
		logger:      logger.Fmt("Syncer"),
		prevIndices: make(map[uuid.UUID]int),
		prevTerms:   make(map[uuid.UUID]int),
		closed:      make(chan struct{}),
		closer:      make(chan struct{}, 1),
	}

	for _, p := range s.root.Peers() {
		s.sync(p)
	}

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
	l.root.Log.head.Notify()
	return err
}

func (s *logSyncer) Append(event Event, source uuid.UUID, seq int, kind int) (item LogItem, err error) {
	var term = s.root.term.Num
	var head int

	committed := make(chan struct{}, 1)
	go func() {
		// append
		head, err = s.root.Log.Append(event, term, source, seq, kind)
		if err != nil {
			s.shutdown(err)
			return
		}

		// wait for majority.
		majority := s.root.Majority()-1
		for done := make(map[uuid.UUID]struct{}); len(done) < majority; {
			for _, p := range s.root.Peers() {
				if _, ok := done[p.id]; ok {
					continue
				}

				index, term := s.GetPrevIndexAndTerm(p.id)
				if index >= head && term == s.root.term.Num {
					done[p.id] = struct{}{}
				}
			}

			if s.Closed() {
				return
			}
		}

		s.root.Log.Commit(head) // commutative, so safe in the event of out of order commits.
		committed <- struct{}{}
	}()

	select {
	case <-s.closed:
		return LogItem{}, common.Or(s.failure, ClosedError)
	case <-committed:
		return LogItem{head, event, term, source, seq, kind}, nil
	}
}

func (s *logSyncer) sync(p peer) error {
	logger := s.logger.Fmt("Routine(%v)", p)
	logger.Info("Starting peer synchronizer")

	var cl *client
	go func() {
		defer logger.Info("Shutting down")
		defer common.RunIf(func() { cl.Close() })(cl)
		var err error

		// snapshot the local log
		max, err := s.root.Log.Max()
		if err != nil {
			s.shutdown(err)
			// FIXME! Better error handling here.
			return
		}

		// the sync'er needs to be unaffected by segment
		// compactions.
		for {
			next, ok := s.root.Log.head.WaitForGreaterThanOrEqual(max.Index + 1)
			if !ok || s.Closed() {
				return
			}

			// loop until this peer is completely caught up to head!
			for max.Index < next {
				if s.Closed() {
					return
				}

				logger.Debug("Currently [%v/%v]", max.Index, next)

				// might have to reinitialize client after each batch.
				if cl == nil {
					cl, err = s.client(p)
					if err != nil {
						return
					}
				}

				// scan a full batch of events.
				batch, err := s.root.Log.Scan(max.Index+1, max.Index+1+256)
				if err != nil {
					s.shutdown(err)
					return
				}

				// send the append request.
				resp, err := cl.Replicate(s.root.Id, s.root.term.Num, max.Index, max.Term, batch, s.root.Log.Committed())
				if err != nil {
					logger.Error("Unable to append events [%v]", err)
					cl = nil
					continue
				}

				// make sure we're still a leader.
				if resp.term > s.root.term.Num {
					logger.Error("No longer leader.")
					s.shutdown(NotLeaderError)
					return
				}

				// if it was successful, progress the peer's index and term
				if resp.success {
					max = batch[len(batch)-1]
					s.SetPrevIndexAndTerm(p.id, max.Index, max.Term)
					continue
				}

				// consistency check failed, start moving backwards one index at a time.
				// TODO: Install snapshot

				max, ok, err = s.root.Log.Get(max.Index - 1)
				if err != nil {
					s.shutdown(err)
					return
				}

				if ok {
					s.SetPrevIndexAndTerm(p.id, max.Index, max.Term)
				} else {
					s.SetPrevIndexAndTerm(p.id, -1, -1)
				}
			}

			logger.Debug("Sync'ed to [%v]", next)
		}
	}()
	return nil
}

func (s *logSyncer) client(p peer) (*client, error) {

	// exponential backoff up to 2^6 seconds.
	for timeout := 1 * time.Second; ; {
		ch := make(chan *client)
		go func() {
			cl, err := p.Client(s.root.Ctx)
			if err == nil && cl != nil {
				ch <- cl
			}
		}()

		timer := time.NewTimer(timeout)
		select {
		case <-s.closed:
			return nil, ClosedError
		case <-timer.C:

			// 64 seconds is maximum timeout
			if timeout < 2^6*time.Second {
				timeout *= 2
			}

			continue
		case cl := <-ch:
			return cl, nil
		}
	}
}

func (s *logSyncer) GetPrevIndexAndTerm(id uuid.UUID) (int, int) {
	s.prevLock.Lock()
	defer s.prevLock.Unlock()
	return s.prevIndices[id], s.prevTerms[id]
}

func (s *logSyncer) SetPrevIndexAndTerm(id uuid.UUID, idx int, term int) {
	s.prevLock.Lock()
	defer s.prevLock.Unlock()
	s.prevIndices[id] = idx
	s.prevTerms[id] = term
}

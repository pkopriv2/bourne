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
	replica.Term(replica.CurrentTerm().num, &replica.Id, &replica.Id)

	logger := replica.Logger.Fmt("Leader(%v)", replica.CurrentTerm())
	logger.Info("Becoming leader")

	l := &leader{
		logger:     logger,
		follower:   follower,
		syncer:     newLogSyncer(replica, logger),
		proxyPool:  concurrent.NewWorkPool(16),
		appendPool: concurrent.NewWorkPool(16),
		term:       replica.CurrentTerm(),
		replica:    replica,
		closed:     make(chan struct{}),
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
			case append := <-l.replica.ProxyMachineAppends:
				l.handleProxyMachineAppend(append)
			}
		}
	}()

	// Main routine
	go func() {
		for {
			timer := time.NewTimer(l.replica.ElectionTimeout / 5)

			select {
			case <-l.replica.closed:
				return
			case <-l.closed:
				return
			case append := <-l.replica.MachineAppends:
				l.handleMachineAppend(append)
			case append := <-l.replica.LogAppends:
				l.handleAppendEvents(append)
			case ballot := <-l.replica.Votes:
				l.handleRequestVote(ballot)
			case <-timer.C:
				l.broadcastHeartbeat()
			case <-l.syncer.closed:
				if l.syncer.failure == NotLeaderError {
					l.transition(l.follower)
					return
				}
				return
			}
		}
	}()
}

func (c *leader) handleProxyMachineAppend(append localAppend) {
	err := c.proxyPool.Submit(func() {
		append.reply(c.replica.MachineAppend(append.event))
	})

	if err != nil {
		append.reply(0, errors.Wrapf(err, "Error submitting work to proxy pool."))
	}
}

func (c *leader) handleMachineAppend(append localAppend) {
	err := c.appendPool.Submit(func() {
		append.reply(c.syncer.Append(append.event))
	})

	if err != nil {
		append.reply(0, errors.Wrapf(err, "Error submitting work to append pool."))
	}
}

func (c *leader) handleRequestVote(vote requestVote) {

	// handle: previous or current term vote.  (immediately decline.  already leader)
	if vote.term <= c.replica.term.num {
		vote.reply(c.replica.term.num, false)
		return
	}

	// handle: future term vote.  (move to new term.  only accept if candidate log is long enough)
	maxLogIndex, maxLogTerm, _ := c.replica.Log.Snapshot()
	if vote.maxLogIndex >= maxLogIndex && vote.maxLogTerm >= maxLogTerm {
		c.replica.Term(vote.term, nil, &vote.id)
		vote.reply(vote.term, true)
	} else {
		c.replica.Term(vote.term, nil, nil)
		vote.reply(vote.term, false)
	}

	c.transition(c.follower)
}

func (c *leader) handleAppendEvents(append appendEvents) {
	if append.term < c.replica.term.num {
		append.reply(c.replica.term.num, false)
		return
	}

	c.replica.Term(append.term, &append.id, &append.id)
	append.reply(append.term, false)
	c.transition(c.follower)
}

func (c *leader) broadcastHeartbeat() {
	ch := c.replica.Broadcast(func(cl *client) response {
		maxLogIndex, maxLogTerm, commit := c.replica.Log.Snapshot()
		c.logger.Info("Sending heart beat (%v,%v,%v)", maxLogIndex, maxLogTerm, commit)

		resp, err := cl.AppendEvents(c.replica.Id, c.replica.term.num, maxLogIndex, maxLogTerm, []Event{}, commit)
		if err != nil {
			return response{c.replica.term.num, false}
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
			if resp.term > c.replica.term.num {
				c.replica.Term(resp.term, nil, c.replica.term.votedFor)
				c.transition(c.follower)
				return
			}

			i++
		case <-timer.C:
			c.replica.Term(c.replica.term.num, nil, c.replica.term.votedFor)
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

	// sets the highwater mark.
	head int

	// conditional lock on head (used to wake synchronizers waiting for work.)
	headLock *sync.Cond

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
		head:        -1,
		headLock:    &sync.Cond{L: &sync.Mutex{}},
		closed:      make(chan struct{}),
		closer:      make(chan struct{}, 1),
	}

	for _, p := range s.root.peers {
		s.sync(p)
	}

	return s
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
	l.headLock.Broadcast()
	return err
}

func (s *logSyncer) moveHead(offset int) {
	s.headLock.L.Lock()
	if offset > s.head {
		s.head = offset
	}
	s.headLock.L.Unlock()
	s.headLock.Broadcast()
}

func (s *logSyncer) getHeadWhenGreater(cur int) (head int, err error) {
	s.headLock.L.Lock()
	defer s.headLock.L.Unlock()

	for head = s.head; head <= cur; head = s.head {
		s.headLock.Wait()
		select {
		default:
			continue
		case <-s.closed:
			return -1, ClosedError
		}
	}
	return
}

func (s *logSyncer) Append(event Event) (head int, err error) {
	select {
	case <-s.closed:
		return 0, ClosedError
	default:
	}

	committed := make(chan struct{}, 1)
	go func() {
		// append
		head = s.root.Log.Append([]Event{event}, s.root.term.num)

		// notify sync'ers
		s.moveHead(head)

		// wait for majority.
		for needed := majority(len(s.root.peers)+1) - 1; needed > 0; {
			for _, p := range s.root.peers {
				index, term := s.GetPrevIndexAndTerm(p.id)
				if index >= head && term == s.root.term.num {
					needed--
				}
			}

			select {
			default:
				time.Sleep(5 * time.Millisecond) // should sleep for expected delivery of one batch. (not 100% sure how to anticipate that.  need to apply RTT techniques)
			case <-s.closed:
				return
			}
		}

		s.root.Log.Commit(head) // commutative, so safe in the event of out of order commits.
		committed <- struct{}{}
	}()

	select {
	case <-s.closed:
		return 0, common.Or(s.failure, ClosedError)
	case <-committed:
		return head, nil
	}
}

func (s *logSyncer) sync(p peer) {
	logger := s.logger.Fmt("Routine(%v)", p)
	logger.Info("Starting peer synchronizer")

	var cl *client
	go func() {
		defer logger.Info("Shutting down")
		defer common.RunIf(func() { cl.Close() })(cl)

		// snapshot the local log
		_, term, commit := s.root.Log.Snapshot()

		// we will start syncing at current commit offset
		prevIndex := commit
		prevTerm := term
		for {
			head, err := s.getHeadWhenGreater(prevIndex)
			if err != nil {
				return
			}

			// loop until this peer is completely caught up to head!
			for prevIndex < head {
				logger.Info("Currently [%v] behind head [%v]", head-prevIndex, head)

				// check for close each time around.
				select {
				default:
				case <-s.closed:
					return
				}

				// might have to reinitialize client after each batch.
				if cl == nil {
					cl, err = s.client(p)
					if err != nil {
						return
					}
				}

				// scan a full batch of events.
				batch := s.root.Log.Scan(prevIndex+1, 256)
				if len(batch) == 0 {
					panic("Inconsistent state!")
				}

				// send the append request.
				resp, err := cl.AppendEvents(s.root.Id, s.root.term.num, prevIndex, prevTerm, eventLogExtractEvents(batch), s.root.Log.Committed())
				if err != nil {
					cl = nil
					continue
				}

				// make sure we're still a leader.
				if resp.term > s.root.term.num {
					logger.Error("No longer leader.")
					s.shutdown(NotLeaderError)
					return
				}

				// if it was successful, progress the peer's index and term
				if resp.success {
					prevIndex += len(batch)
					prevTerm = s.root.term.num
					s.SetPrevIndexAndTerm(p.id, prevIndex, prevTerm)

					// accumulates some updates
					time.Sleep(20 * time.Millisecond)
					continue
				}

				// consistency check failed, start moving backwards one index at a time.
				// TODO: Implement optimization to come to faster agreement.
				prevIndex -= 1
				if prevIndex < -1 {
					logger.Error("Unable to sync peer log")
					return
				}

				if prevItem, ok := s.root.Log.Get(prevIndex); ok {
					s.SetPrevIndexAndTerm(p.id, prevIndex, prevItem.term)
				} else {
					s.SetPrevIndexAndTerm(p.id, -1, -1)
				}
			}

			logger.Debug("Sync'ed to [%v]", head)
		}
	}()
}

func (s *logSyncer) client(p peer) (*client, error) {

	// exponential backoff up to 2^6 seconds.
	for timeout := 1 * time.Second; ; {
		ch := make(chan *client)
		go func() {
			cl, err := p.Client(s.root.Ctx, s.root.Parser)
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
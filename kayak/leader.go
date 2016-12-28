package kayak

import (
	"sync"
	"time"

	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

type leader struct {
	ctx      common.Context
	in       chan *instance
	follower chan<- *instance
	closed   chan struct{}
}

func newLeader(ctx common.Context, in chan *instance, follower chan<- *instance, closed chan struct{}) *leader {
	ret := &leader{ctx, in, follower, closed}
	ret.start()
	return ret
}

func (c *leader) start() error {
	go func() {
		for {
			select {
			case <-c.closed:
				return
			case i := <-c.in:
				c.run(i)
			}
		}
	}()
	return nil
}

func (c *leader) send(h *instance, ch chan<- *instance) error {
	select {
	case <-c.closed:
		return ClosedError
	case ch <- h:
		return nil
	}
}

func (c *leader) run(h *instance) error {
	logger := h.logger.Fmt("Leader(%v)", h.term)
	logger.Info("Becoming leader")

	// become leader for current term.
	h.Term(h.term.num, &h.id, &h.id)

	// establish leadership
	if next := c.handleHeartbeatTimeout(logger, h); next != nil {
		c.send(h, next)
		return nil
	}

	// start the log syncer.
	sync := newLogSyncer(h, logger)

	go func() {
		for {
			logger.Info("Resetting heartbeat timer [%v]", h.timeout/5)
			timer := time.NewTimer(h.timeout / 5)

			select {
			case <-c.closed:
				return
			case append := <-h.clientAppends:
				if next := c.handleClientAppend(sync, append); next != nil {
					c.send(h, next)
					return
				}
			case append := <-h.appends:
				if next := c.handleAppendEvents(h, append); next != nil {
					c.send(h, next)
					return
				}
			case ballot := <-h.votes:
				if next := c.handleRequestVote(h, ballot); next != nil {
					c.send(h, next)
					return
				}
			case <-timer.C:
				if next := c.handleHeartbeatTimeout(logger, h); next != nil {
					c.send(h, next)
					return
				}
			}
		}
	}()
	return nil
}

func (c *leader) handleClientAppend(s *logSyncer, a clientAppend) chan<- *instance {
	a.reply(s.Append(a.events))
	return nil
}

func (c *leader) handleRequestVote(h *instance, vote requestVote) chan<- *instance {

	// handle: previous or current term vote.  (immediately decline.  already leader)
	if vote.term <= h.term.num {
		vote.reply(h.term.num, false)
		return nil
	}

	// handle: future term vote.  (move to new term.  only accept if candidate log is long enough)
	maxLogIndex, maxLogTerm, _ := h.log.Snapshot()
	if vote.maxLogIndex >= maxLogIndex && vote.maxLogTerm >= maxLogTerm {
		defer h.Term(vote.term, nil, &vote.id)
		vote.reply(vote.term, true)
	} else {
		defer h.Term(vote.term, nil, nil)
		vote.reply(vote.term, false)
	}

	return c.follower
}

func (c *leader) handleAppendEvents(h *instance, append appendEvents) chan<- *instance {
	if append.term < h.term.num {
		append.reply(h.term.num, false)
		return nil
	}

	defer h.Term(append.term, &append.id, &append.id)
	append.reply(append.term, false)
	return c.follower
}

func (c *leader) handleHeartbeatTimeout(logger common.Logger, h *instance) chan<- *instance {
	ch := h.Broadcast(func(cl *client) response {
		maxLogIndex, maxLogTerm, commit := h.log.Snapshot()
		logger.Info("Sending heart beat (%v,%v,%v)", maxLogIndex, maxLogTerm, commit)
		resp, err := cl.AppendEvents(h.id, h.term.num, maxLogIndex, maxLogTerm, []event{}, commit)
		if err != nil {
			return response{h.term.num, false}
		} else {
			return resp
		}
	})

	timer := time.NewTimer(h.timeout)
	for i := 0; i < h.Majority()-1; {
		select {
		case <-c.closed:
			return nil
		case resp := <-ch:
			if resp.term > h.term.num {
				h.Term(resp.term, nil, nil)
				return c.follower
			}

			i++
		case <-timer.C:
			h.Term(h.term.num, nil, nil)
			return c.follower
		}
	}

	return nil
}

// the log syncer should be rebuilt every time a leader comes to power.
type logSyncer struct {

	// the primary member instance. (guaranteed to be immutable)
	root *instance

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

func newLogSyncer(inst *instance, logger common.Logger) *logSyncer {
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
	if s.head < offset {
		s.head = offset
	}
	s.headLock.L.Unlock()
	s.headLock.Broadcast()
}

func (s *logSyncer) getHeadWhenGreater(cur int) (head int, err error) {
	s.headLock.L.Lock()
	for head = s.head; head <= cur; head = s.head {
		s.headLock.Wait()
		select {
		default:
			continue
		case <-s.closed:
			return -1, ClosedError
		}
	}
	s.headLock.L.Unlock()
	return
}

func (s *logSyncer) Append(batch []event) (err error) {
	select {
	case <-s.closed:
		return ClosedError
	default:
	}

	// if nothing was sent, just return immediately
	if len(batch) == 0 {
		return nil
	}

	// append to local log
	head := s.root.log.Append(batch, s.root.term.num)
	s.moveHead(head)

	committed := make(chan struct{}, 1)
	go func() {
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

		// data race if multiple threads are allowed to append - which they currently are
		// not allowed to do.
		committed <- struct{}{}
	}()

	timer := time.NewTimer(30 * time.Second)
	select {
	case <-s.closed:
		return ClosedError
	case <-timer.C:
		return TimeoutError
	case <-committed:
		s.root.log.Commit(head)
		return nil
	}
}

func (s *logSyncer) sync(p peer) {
	logger := s.logger.Fmt("%v", p)
	logger.Info("Starting peer synchronizer")
	go func() {
		var cl *client
		var err error
		defer common.RunIf(func() { cl.Close() })(cl)

		// snapshot the local log
		_, term, head := s.root.log.Snapshot()

		// we will start syncing at current commit offset
		prevIndex := head
		prevTerm := term
		for {
			head, err = s.getHeadWhenGreater(prevIndex)
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
				batch := s.root.log.Scan(prevIndex+1, 256)
				if len(batch) == 0 {
					panic("Inconsistent state!")
				}

				// send the append request.
				resp, err := cl.AppendEvents(s.root.id, s.root.term.num, prevIndex, prevTerm, batch, s.root.log.Committed())
				if err != nil {
					cl = nil
					continue
				}

				// make sure we're still a leader.
				if resp.term > s.root.term.num {
					s.shutdown(NotLeaderError)
					return
				}

				// if it was successful, progress the peers index and term
				if resp.success {
					prevIndex += len(batch)
					prevTerm = s.root.term.num
					s.SetPrevIndexAndTerm(p.id, prevIndex, prevTerm)
					continue
				}

				// consistency check failed, start moving backwards one index at a time.
				// TODO: Implement optimization to come to faster agreement.
				prevIndex -= 1
				if prevItem, ok := s.root.log.Get(prevIndex); ok {
					s.SetPrevIndexAndTerm(p.id, prevIndex, prevItem.term)
				} else {
					s.SetPrevIndexAndTerm(p.id, -1, -1)
				}
			}
		}
	}()
}

func (s *logSyncer) client(p peer) (*client, error) {

	// exponential backoff up to 2^6 seconds.
	for timeout := 1 * time.Second; ; {
		ch := make(chan *client)
		go func() {
			cl, err := p.Client(s.root.ctx, s.root.parser)
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

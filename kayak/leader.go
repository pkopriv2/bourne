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

	// start the log syncer.
	sync := newLogSyncer(h, logger)

	go func() {
		for {
			logger.Debug("Resetting heartbeat timer [%v]", h.timeout/5)
			timer := time.NewTimer(h.timeout / 3)

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
				if next := c.handleHeartbeatTimeout(h); next != nil {
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

func (c *leader) handleHeartbeatTimeout(h *instance) chan<- *instance {
	ch := h.Broadcast(func(cl *client) response {
		maxLogIndex, maxLogTerm, commit := h.log.Snapshot()
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

	// sets the highwater mark.  (used to wake sleeping sync threads)
	head     int
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

	defer close(l.closed)
	defer common.RunIf(func() { l.failure = err })(err)
	return err
}

func (s *logSyncer) moveHead(offset int) {
	s.headLock.L.Lock()
	if s.head < offset {
		s.head = offset
	}
	s.headLock.L.Unlock()
}

func (s *logSyncer) getHeadWhenGreater(cur int) (head int) {
	s.headLock.L.Lock()
	for head = s.head; cur >= head; {
		s.headLock.Wait()
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
				time.Sleep(20 * time.Millisecond) // should sleep for expected delivery of one batch.
			case <-s.closed:
				return
			}
		}

		// data race if multiple threads are allowed to append - whcih they currently are
		// not allowed to do.
		s.root.log.Commit(head)
		committed <- struct{}{}
	}()

	timer := time.NewTimer(30 * time.Second)
	select {
	case <-s.closed:
		return ClosedError
	case <-timer.C:
		return TimeoutError
	case <-committed:
		return nil
	}
}

func (s *logSyncer) sync(p peer) {
	s.logger.Info("Starting log synchronizer for [%v]", p)
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
			head := s.getHeadWhenGreater(prevIndex)

			// loop until this peer is completely caught up to head!
			for prevIndex < head {
				s.logger.Debug("Currently [%v] behind head [%v]", head-prevIndex, head)

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
				resp, err := cl.AppendEvents(s.root.id, term, prevIndex, prevTerm, batch, s.root.log.Committed())
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
					prevTerm = term

					s.SetPrevIndexAndTerm(p.id, prevIndex, prevTerm)
					continue
				}

				// consistency check failed, start moving backwards one index at a time.
				// TODO: Implement optimization to come to faster agreement.
				prevIndex -= 1
				prevItem := s.root.log.Get(prevIndex)
				s.SetPrevIndexAndTerm(p.id, prevIndex, prevItem.term)
			}
		}
	}()
}

func (s *logSyncer) client(p peer) (*client, error) {

	// exponential backoff up to 2^6 seconds.
	for timeout := 1 * time.Second; ; {
		ch := make(chan *client)
		go func() {
			cl, err := p.Client(s.root.ctx)
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

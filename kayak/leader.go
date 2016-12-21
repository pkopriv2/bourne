package kayak

import (
	"time"

	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

type leader struct {
	ctx      common.Context
	logger   common.Logger
	in       chan *instance
	follower chan<- *instance
	closed   chan struct{}
}

func newLeader(ctx common.Context, logger common.Logger, in chan *instance, follower chan<- *instance, closed chan struct{}) *leader {
	ret := &leader{ctx, logger.Fmt("Leader:"), in, follower, closed}
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
	logger := c.logger.Fmt("%v", h)
	logger.Info("Becoming leader")

	// become leader for current term.
	h.Term(h.term.num, &h.id, &h.id)

	go func() {
		for {

			logger.Debug("Resetting heartbeat timer [%v]", h.timeout)
			timer := time.NewTimer(h.timeout)

			select {
			case <-c.closed:
				return
			case append := <-h.clientAppends:
				if next := c.handleClientAppend(h, append); next != nil {
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
				return
			}
		}
	}()
	return nil
}

func (c *leader) handleClientAppend(h *instance, append clientAppend) chan<- *instance {
	//
	return nil
}

func (c *leader) handleRequestVote(h *instance, vote requestVote) chan<- *instance {

	// handle: previous or current term vote.  (immediately decline.  already leader)
	if vote.term <= h.term.num {
		vote.reply(h.term.num, false)
		return nil
	}

	// handle: current term vote.  (accept if no vote and if candidate log is as long as ours)
	maxLogIndex, maxLogTerm, _ := h.log.Snapshot()

	// handle: future term vote.  (move to new term.  only accept if candidate log is long enough)
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

type logSyncer struct {
	// root *member

	// tracks follower states when leader
	offsets map[uuid.UUID]int
	commits map[uuid.UUID]int

	// tracks which clients are being caught up
	behind map[uuid.UUID]struct{}

	// the other peers. (currently static list)
	peers []peer

	closed chan struct{}
}

func (s *logSyncer) sync() error {
	return nil
}

func (s *logSyncer) Append(c clientAppend) error {
	return nil
}

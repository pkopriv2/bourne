package kayak

import (
	"time"

	"github.com/pkopriv2/bourne/common"
)

type candidate struct {
	ctx      common.Context
	in       chan *instance
	leader   chan<- *instance
	follower chan<- *instance
	closed   chan struct{}
}

func newCandidate(ctx common.Context, in chan *instance, leader chan<- *instance, follower chan<- *instance, closed chan struct{}) *candidate {
	ret := &candidate{ctx, in, leader, follower, closed}
	ret.start()
	return ret
}

func (c *candidate) start() error {
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

func (c *candidate) send(h *instance, ch chan<- *instance) error {
	select {
	case <-c.closed:
		return ClosedError
	case ch <- h:
		return nil
	}
}

func (c *candidate) run(h *instance) error {

	// increment term and vote forself.
	h.Term(h.term.num+1, nil, &h.id)

	// decorate the logger
	logger := h.logger.Fmt("Candidate(%v)", h.term)
	logger.Info("Becoming candidate")

	// send out ballots
	ballots := h.Broadcast(func(cl *client) response {
		maxLogIndex, maxLogTerm, _ := h.log.Snapshot()
		resp, err := cl.RequestVote(h.id, h.term.num, maxLogIndex, maxLogTerm)
		if err != nil {
			return response{h.term.num, false}
		} else {
			return resp
		}
	})

	// set the election timer.
	logger.Info("Setting timer [%v]", h.timeout)
	timer := time.NewTimer(h.timeout)

	// kick off candidate routine
	go func() {
		for numVotes := 1; ; {
			needed := h.Majority()

			logger.Info("Received [%v/%v] votes", numVotes, len(h.peers)+1)
			if numVotes >= needed {
				h.Term(h.term.num, &h.id, &h.id)
				c.send(h, c.leader)
				return

			}

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
				logger.Info("Unable to acquire necessary votes [%v/%v]", numVotes, needed)
				c.send(h, c.in) // becomes a new candidate
				return
			case vote := <-ballots:
				if vote.term > h.term.num {
					h.Term(vote.term, nil, nil)
					c.send(h, c.follower)
					return
				}

				if vote.success {
					numVotes++
				}
			}
		}
	}()

	return nil

}

func (c *candidate) handleClientAppend(h *instance, a clientAppend) chan<- *instance {
	a.reply(NotLeaderError)
	return nil
}

func (c *candidate) handleRequestVote(h *instance, vote requestVote) chan<- *instance {
	if vote.term <= h.term.num {
		vote.reply(h.term.num, false)
		return nil
	}

	defer h.Term(vote.term, nil, &vote.id)
	vote.reply(vote.term, true)
	return c.follower
}

func (c *candidate) handleAppendEvents(h *instance, append appendEvents) chan<- *instance {
	if append.term < h.term.num {
		append.reply(h.term.num, false)
		return nil
	}

	// append.term is >= term.  use it from now on.
	defer h.Term(append.term, &append.id, &append.id)
	append.reply(append.term, false)
	return c.follower
}

package kayak

import (
	"time"

	"github.com/pkopriv2/bourne/common"
)

type candidate struct {
	ctx      common.Context
	in       chan *replica
	leader   chan<- *replica
	follower chan<- *replica
	closed   chan struct{}
}

func newCandidate(ctx common.Context, in chan *replica, leader chan<- *replica, follower chan<- *replica, closed chan struct{}) *candidate {
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

func (c *candidate) transition(h *replica, ch chan<- *replica) error {
	select {
	case <-c.closed:
		return ClosedError
	case ch <- h:
		return nil
	}
}

func (c *candidate) run(h *replica) error {

	// increment term and vote forself.
	h.Term(h.term.num+1, nil, &h.Id)

	// decorate the logger
	logger := h.Logger.Fmt("Candidate(%v)", h.term)
	logger.Info("Becoming candidate")

	// send out ballots
	ballots := h.Broadcast(func(cl *client) response {
		maxLogIndex, maxLogTerm, _ := h.Log.Snapshot()
		resp, err := cl.RequestVote(h.Id, h.term.num, maxLogIndex, maxLogTerm)
		if err != nil {
			return response{h.term.num, false}
		} else {
			return resp
		}
	})

	// set the election timer.
	logger.Info("Setting timer [%v]", h.ElectionTimeout)
	timer := time.NewTimer(h.ElectionTimeout)

	// kick off candidate routine
	go func() {
		for numVotes := 1; ; {
			needed := h.Majority()

			logger.Info("Received [%v/%v] votes", numVotes, len(h.peers)+1)
			if numVotes >= needed {
				logger.Info("Acquired majority [%v] votes.", needed)
				h.Term(h.term.num, &h.Id, &h.Id)
				c.transition(h, c.leader)
				return

			}

			select {
			case <-c.closed:
				return
			case append := <-h.Appends:
				if next := c.handleAppendEvents(h, append); next != nil {
					c.transition(h, next)
					return
				}
			case ballot := <-h.Votes:
				if next := c.handleRequestVote(h, ballot); next != nil {
					c.transition(h, next)
					return
				}
			case <-timer.C:
				logger.Info("Unable to acquire necessary votes [%v/%v]", numVotes, needed)
				c.transition(h, c.in) // becomes a new candidate
				return
			case vote := <-ballots:
				if vote.term > h.term.num {
					h.Term(vote.term, nil, nil)
					c.transition(h, c.follower)
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

func (c *candidate) handleRequestVote(h *replica, vote requestVote) chan<- *replica {
	if vote.term <= h.term.num {
		vote.reply(h.term.num, false)
		return nil
	}

	defer h.Term(vote.term, nil, &vote.id)
	vote.reply(vote.term, true)
	return c.follower
}

func (c *candidate) handleAppendEvents(h *replica, append appendEvents) chan<- *replica {
	if append.term < h.term.num {
		append.reply(h.term.num, false)
		return nil
	}

	// append.term is >= term.  use it from now on.
	defer h.Term(append.term, &append.id, &append.id)
	append.reply(append.term, false)
	return c.follower
}

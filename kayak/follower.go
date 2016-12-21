package kayak

import (
	"time"

	"github.com/pkopriv2/bourne/common"
)

type follower struct {
	ctx       common.Context
	in        chan *instance
	candidate chan<- *instance
	closed    chan struct{}
}

func newFollower(ctx common.Context, in chan *instance, candidate chan<- *instance, closed chan struct{}) *follower {
	ret := &follower{ctx, in, candidate, closed}
	ret.start()
	return ret
}

func (c *follower) start() error {
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

func (c *follower) send(h *instance, ch chan<- *instance) error {
	select {
	case <-c.closed:
		return ClosedError
	case ch <- h:
		return nil
	}
}

func (c *follower) run(h *instance) error {
	logger := h.logger.Fmt("Follower[%v]", h.term)
	logger.Info("Becoming follower")

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
				logger.Info("Waited too long for heartbeat.")
				c.send(h, c.candidate) // becomes a new candidate
				return
			}
		}
	}()
	return nil
}

func (c *follower) handleClientAppend(h *instance, append clientAppend) chan<- *instance {
	append.reply(NotLeaderError)
	return nil
}

func (c *follower) handleRequestVote(h *instance, vote requestVote) chan<- *instance {

	// handle: previous term vote.  (immediately decline.)
	if vote.term < h.term.num {
		vote.reply(h.term.num, false)
		return nil
	}

	// handle: current term vote.  (accept if no vote and if candidate log is as long as ours)
	maxLogIndex, maxLogTerm, _ := h.log.Snapshot()
	if vote.term == h.term.num {
		if h.term.votedFor == nil && vote.maxLogIndex >= maxLogIndex && vote.maxLogTerm >= maxLogTerm {
			h.Term(h.term.num, nil, &vote.id) // correct?
			vote.reply(h.term.num, true)
		} else {
			vote.reply(h.term.num, false)
		}

		return nil
	}

	// handle: future term vote.  (move to new term.  only accept if candidate log is long enough)
	if vote.maxLogIndex >= maxLogIndex && vote.maxLogTerm >= maxLogTerm {
		vote.reply(vote.term, true)
		h.Term(vote.term, nil, &vote.id)
		return nil
	} else {
		vote.reply(vote.term, false)
		h.Term(vote.term, nil, nil)
		return nil
	}
}

func (c *follower) handleAppendEvents(h *instance, append appendEvents) chan<- *instance {
	logger := h.logger.Fmt("Follower[%v]", h.term)

	if append.term < h.term.num {
		append.reply(h.term.num, false)
		return nil
	}

	if append.term > h.term.num || h.term.leader == nil {
		logger.Info("New leader detected [%v]", append.id)
		append.reply(append.term, false)
		h.Term(append.term, &append.id, &append.id)
		return nil
	}

	logTerm, _ := h.log.Get(append.prevLogIndex)
	if logTerm != append.prevLogTerm {
		logger.Info("Inconsistent log detected. Rolling back")
		append.reply(append.term, false)
		return nil
	}

	append.reply(append.term, true)
	h.log.Insert(append.events, append.prevLogIndex+1, append.term)
	return nil
}

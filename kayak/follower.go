package kayak

import (
	"time"

	"github.com/pkopriv2/bourne/common"
)

type follower struct {
	ctx       common.Context
	in        chan *member
	candidate chan<- *member
	closed    chan struct{}
}

func newFollower(ctx common.Context, in chan *member, candidate chan<- *member, closed chan struct{}) *follower {
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

func (c *follower) transition(h *member, ch chan<- *member) error {
	select {
	case <-c.closed:
		return ClosedError
	case ch <- h:
		return nil
	}
}

func (c *follower) run(h *member) error {
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
				if next := c.handleClientAppend(h, logger, append); next != nil {
					c.transition(h, next)
					return
				}
			case append := <-h.appends:
				if next := c.handleAppendEvents(h, logger, append); next != nil {
					c.transition(h, next)
					return
				}
			case ballot := <-h.votes:
				if next := c.handleRequestVote(h, logger, ballot); next != nil {
					c.transition(h, next)
					return
				}
			case <-timer.C:
				logger.Info("Waited too long for heartbeat.")
				c.transition(h, c.candidate) // becomes a new candidate
				return
			}
		}
	}()
	return nil
}

func (c *follower) handleClientAppend(h *member, logger common.Logger, append clientAppend) chan<- *member {
	append.reply(NotLeaderError)
	return nil
}

func (c *follower) handleRequestVote(h *member, logger common.Logger, vote requestVote) chan<- *member {
	logger.Debug("Handling request vote [%v]", vote)

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
			return c.in
		}

		vote.reply(h.term.num, false)
		return nil
	}

	// handle: future term vote.  (move to new term.  only accept if candidate log is long enough)
	if vote.maxLogIndex >= maxLogIndex && vote.maxLogTerm >= maxLogTerm {
		vote.reply(vote.term, true)
		h.Term(vote.term, nil, &vote.id)
	} else {
		vote.reply(vote.term, false)
		h.Term(vote.term, nil, nil)
	}

	return c.in
}

func (c *follower) handleAppendEvents(h *member, logger common.Logger, append appendEvents) chan<- *member {
	if append.term < h.term.num {
		append.reply(h.term.num, false)
		return nil
	}

	if append.term > h.term.num || h.term.leader == nil {
		logger.Info("New leader detected [%v]", append.id)
		append.reply(append.term, false)
		h.Term(append.term, &append.id, &append.id)
		return c.in
	}

	if logItem, ok := h.log.Get(append.prevLogIndex); ok && logItem.term != append.prevLogTerm {
		logger.Info("Inconsistent log detected [%v,%v]. Rolling back", logItem.term, append.prevLogTerm)
		append.reply(append.term, false)
		return nil
	}

	h.log.Insert(append.events, append.prevLogIndex+1, append.term)
	h.log.Commit(append.commit)
	append.reply(append.term, true)
	return nil
}

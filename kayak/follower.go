package kayak

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

// The follower machine.  This
type follower struct {
	ctx     common.Context
	logger  common.Logger
	ctrl    common.Control
	term    term
	replica *replica
}

func becomeFollower(replica *replica) {
	ctx := replica.Ctx.Sub("Follower(%v)", replica.CurrentTerm())
	ctx.Logger().Info("Becoming follower")
	l := &follower{
		ctx:     ctx,
		logger:  ctx.Logger(),
		ctrl:    ctx.Control(),
		term:    replica.CurrentTerm(),
		replica: replica,
	}
	l.start()
}

func (c *follower) start() {
	// Proxy routine. (Out of band to allow replication requests through faster)
	go func() {
		defer c.ctrl.Close()
		for {
			select {
			case <-c.ctrl.Closed():
				return
			case req := <-c.replica.LocalAppends:
				c.handleLocalAppend(req)
			case req := <-c.replica.RemoteAppends:
				c.handleRemoteAppend(req)
			case req := <-c.replica.RosterUpdates:
				c.handleRosterUpdate(req)
			case req := <-c.replica.Barrier:
				c.handleReadBarrier(req)
			}
		}
	}()

	// Main routine
	go func() {
		defer c.ctrl.Close()

		var snapshotStream chan<- Event
		var snapshotDone *common.Request
		var snapshotOffset int

		timer := time.NewTimer(c.replica.ElectionTimeout)
		c.logger.Debug("Resetting election timeout: %v", c.replica.ElectionTimeout)
		for ! c.ctrl.IsClosed() {
			select {
			case <-c.ctrl.Closed():
				return
			case <-c.replica.ctrl.Closed():
				return
			case req := <-c.replica.Replications:
				c.handleReplication(req)
			case req := <-c.replica.VoteRequests:
				c.handleRequestVote(req)
			case req := <-c.replica.Snapshots:
				snapshotStream, snapshotDone, snapshotOffset = c.handleInstallSnapshot(req, snapshotStream, snapshotDone, snapshotOffset)
			case <-timer.C:
				c.logger.Info("Waited too long for heartbeat.")
				becomeCandidate(c.replica)
				return
			}

			timer.Reset(c.replica.ElectionTimeout)
		}
	}()
}

func (c *follower) handleLocalAppend(req *common.Request) {
	req.Fail(NotLeaderError)
}

func (c *follower) handleReadBarrier(req *common.Request) {
	req.Fail(NotLeaderError)
}

func (c *follower) handleRemoteAppend(req *common.Request) {
	req.Fail(NotLeaderError)
}

func (c *follower) handleRosterUpdate(req *common.Request) {
	req.Fail(NotLeaderError)
}

func (c *follower) handleInstallSnapshot(req *common.Request, data chan<- Event, done *common.Request, offset int) (chan<- Event, *common.Request, int) {
	segment := req.Body().(installSnapshot)
	if segment.term < c.term.Num {
		req.Ack(newResponse(c.term.Num, false))
		return data, done, offset
	}

	if data == nil {
		data, done = c.startSnapshotStream(segment)
		offset = 0
	}

	if segment.batchOffset != offset {
		close(data)
		data, done = c.startSnapshotStream(segment)
		offset = 0
	}

	c.logger.Info("Installing snapshot: %v", segment)
	if err := c.streamSnapshotSegment(data, segment); err != nil {
		req.Fail(err)
		return data, done, offset
	}

	c.logger.Error("Successfully installed segment: [%v/%v]", offset, segment.size)

	offset += len(segment.batch)
	if offset < segment.size {
		req.Ack(newResponse(c.term.Num, true))
		return data, done, offset
	}

	select {
	case r := <-done.Acked():
		req.Ack(r)
	case e := <-done.Failed():
		req.Fail(e)
	case <-c.ctrl.Closed():
		req.Fail(ClosedError)
	}

	return nil, nil, 0
}

func (c *follower) handleRequestVote(req *common.Request) {
	vote := req.Body().(requestVote)

	c.logger.Debug("Handling request vote [%v]", vote)

	// FIXME: Lots of duplicates here....condense down

	// handle: previous term vote.  (immediately decline.)
	if vote.term < c.term.Num {
		req.Ack(newResponse(c.term.Num, false))
		return
	}

	// handle: current term vote.  (accept if no vote and if candidate log is as long as ours)
	maxIndex, maxTerm, err := c.replica.Log.Last()
	if err != nil {
		req.Ack(newResponse(c.term.Num, false))
		return
	}

	c.logger.Debug("Current log max: %v", maxIndex)
	if vote.term == c.term.Num {
		if c.term.VotedFor == nil && vote.maxLogIndex >= maxIndex && vote.maxLogTerm >= maxTerm {
			c.logger.Debug("Voting for candidate [%v]", vote.id)
			req.Ack(newResponse(c.term.Num, true))
			c.replica.Term(c.term.Num, nil, &vote.id) // correct?
			becomeFollower(c.replica)
			c.ctrl.Close()
			return
		}

		c.logger.Debug("Rejecting candidate vote [%v]", vote.id)
		req.Ack(newResponse(c.term.Num, false))
		becomeCandidate(c.replica)
		c.ctrl.Close()
		return
	}

	// handle: future term vote.  (move to new term.  only accept if candidate log is long enough)
	if vote.maxLogIndex >= maxIndex && vote.maxLogTerm >= maxTerm {
		c.logger.Debug("Voting for candidate [%v]", vote.id)
		req.Ack(newResponse(vote.term, true))
		c.replica.Term(vote.term, nil, &vote.id)
		becomeFollower(c.replica)
		c.ctrl.Close()
		return
	}

	c.logger.Debug("Rejecting candidate vote [%v]", vote.id)
	req.Ack(newResponse(vote.term, false))
	c.replica.Term(vote.term, nil, nil)
	becomeCandidate(c.replica)
	c.ctrl.Close()
}

func (c *follower) handleReplication(req *common.Request) {
	append := req.Body().(replicate)

	if append.term < c.term.Num {
		req.Ack(newResponse(c.term.Num, true))
		return
	}

	hint, _, err := c.replica.Log.Last()
	if err != nil {
		req.Fail(err)
		return
	}

	c.logger.Debug("Handling replication: %v", append)
	if append.term > c.term.Num || c.term.Leader == nil {
		c.logger.Info("New leader detected [%v]", append.id)
		req.Ack(newResponseWithHint(append.term, false, hint))
		c.replica.Term(append.term, &append.id, &append.id)
		becomeFollower(c.replica)
		c.ctrl.Close()
		return
	}

	// if this is a heartbeat, bail out
	c.replica.Log.Commit(append.commit)
	if len(append.items) == 0 {
		req.Ack(newResponse(append.term, true))
		return
	}

	// consistency check
	ok, err := c.replica.Log.Assert(append.prevLogIndex, append.prevLogTerm)
	if err != nil {
		req.Fail(err)
		return
	}

	// consistency check failed.
	if !ok {
		c.logger.Debug("Consistency check failed. Responding with hint [%v]", hint)
		req.Ack(newResponseWithHint(append.term, false, hint))
		return
	}

	// insert items.
	c.replica.Log.Truncate(append.prevLogIndex + 1)
	if err := c.replica.Log.Insert(append.items); err != nil {
		c.logger.Error("Error inserting batch: %v", err)
		req.Fail(err)
		return
	}

	req.Ack(newResponse(append.term, true))
}

func (c *follower) startSnapshotStream(s installSnapshot) (chan<- Event, *common.Request) {
	data := make(chan Event)
	resp := common.NewRequest(nil)
	go func() {
		snapshot, err := c.replica.Log.NewSnapshot(s.maxIndex, s.maxTerm, data, s.size, s.config)
		if err != nil {
			c.logger.Error("Error installing snapshot: %+v", err)
			resp.Fail(err)
			return
		}

		err = c.replica.Log.Install(snapshot)
		if err != nil {
			c.logger.Error("Error installing snapshot: %+v", err)
			resp.Fail(err)
			return
		}

		resp.Ack(newResponse(c.term.Num, true))
	}()
	return data, resp
}

func (c *follower) streamSnapshotSegment(data chan<- Event, s installSnapshot) error {
	timer := time.NewTimer(c.replica.RequestTimeout)
	for i := 0; i < len(s.batch); i++ {
		select {
		case <-timer.C:
			return errors.Wrapf(common.TimeoutError, "Timed out writing segment [%v]: [%v]", c.replica.RequestTimeout, s)
		case <-c.ctrl.Closed():
			return errors.Wrapf(common.ClosedError, "Unable to stream segment [%v]", s)
		case data <- s.batch[i]:
		}
	}
	return nil
}

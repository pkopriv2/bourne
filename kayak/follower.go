package kayak

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/net"
)

// The follower machine.  This
type follower struct {
	ctx          common.Context
	logger       common.Logger
	ctrl         common.Control
	term         term
	replica      *replica
	proxyPool    concurrent.WorkPool
	snapshotPool concurrent.WorkPool
	appendPool   concurrent.WorkPool
	clientPool   net.ClientPool
	// connPool     net.ConnectionPool
}

func becomeFollower(replica *replica) {
	ctx := replica.Ctx.Sub("Follower(%v)", replica.CurrentTerm())
	ctx.Logger().Info("Becoming follower")

	var clientPool net.ClientPool
	if leader := replica.Leader(); leader != nil {
		clientPool = net.NewClientPool(ctx, ctx.Logger(), leader.Pool(replica.Ctx))
	}

	l := &follower{
		ctx:        ctx,
		logger:     ctx.Logger(),
		ctrl:       ctx.Control(),
		proxyPool:  concurrent.NewWorkPool(16),
		appendPool: concurrent.NewWorkPool(16),
		clientPool: clientPool,
		term:       replica.CurrentTerm(),
		replica:    replica,
	}

	l.start()
}

func (c *follower) start() {
	// If any config has changed, just restart the follower...
	_, ver := c.replica.Roster.Get()
	go func() {
		defer c.logger.Info("Closing config watcher.")
		defer c.ctrl.Close()

		_, _, ok := c.replica.Roster.Wait(ver)
		if c.ctrl.IsClosed() || !ok {
			return
		}

		becomeFollower(c.replica)
		return
	}()

	// Proxy routine. (out of band to prevent deadlocks between state machine and replicated log)
	if leader := c.replica.Leader(); leader != nil {
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
				}
			}
		}()
	}

	// Main routine
	go func() {
		defer c.ctrl.Close()
		for {
			electionTimer := time.NewTimer(c.replica.ElectionTimeout)
			c.logger.Debug("Resetting election timeout: %v", c.replica.ElectionTimeout)

			select {
			case <-c.ctrl.Closed():
				return
			case req := <-c.replica.Replications:
				c.handleReplication(req)
			case req := <-c.replica.VoteRequests:
				c.handleRequestVote(req)
			case req := <-c.replica.Snapshots:
				c.handleInstallSnapshot(req)
			case <-electionTimer.C:
				c.logger.Info("Waited too long for heartbeat.")
				becomeCandidate(c.replica)
				return
			}
		}
	}()
}

func (c *follower) handleLocalAppend(req stdRequest) {
	append := req.Body().(appendEvent)

	timeout := c.replica.RequestTimeout / 2

	err := c.proxyPool.SubmitTimeout(timeout, func() {
		cl := c.clientPool.TakeTimeout(timeout)
		if cl == nil {
			req.Fail(common.NewTimeoutError(timeout, "Error retrieving connection from pool."))
			return
		}

		i, e := newClient(cl).Append(append.Event, append.Source, append.Seq, append.Kind)
		if e == nil {
			c.clientPool.Return(cl)
		} else {
			c.clientPool.Fail(cl)
		}
		req.Return(i, e)
	})
	if err != nil {
		req.Fail(err)
	}
}

func (c *follower) handleRemoteAppend(req stdRequest) {
	req.Fail(NotLeaderError)
}

func (c *follower) handleRosterUpdate(req stdRequest) {
	req.Fail(NotLeaderError)
}

func (c *follower) handleInstallSnapshot(req stdRequest) {
	// OH MY GOD....THIS IS COMPLEX!  This can be mostly inlined in the main loop.
	segment := req.Body().(installSnapshot)
	if segment.term < c.term.Num {
		req.Ack(newResponse(c.term.Num, false))
		return
	}

	c.logger.Info("Installing snapshot: %v", segment)
	if segment.batchOffset != 0 {
		req.Ack(newResponse(c.term.Num, false))
		return
	}

	data := make(chan Event)
	resp := newStdRequest(nil)
	go func(s installSnapshot) {
		snapshot, err := c.replica.Log.NewSnapshot(s.maxIndex, s.maxTerm, data, s.size, s.config)
		if err != nil {
			c.logger.Error("Error creating snapshot: %v", err)
			resp.Fail(err)
			return
		}

		success, err := c.replica.Log.Install(snapshot)
		if err != nil {
			c.logger.Error("Error installing snapshot: %v", err)
			resp.Fail(err)
			return
		}

		resp.Ack(newResponse(c.term.Num, success))
	}(segment)

	defer close(data)
	streamSegment := func(s installSnapshot) error {
		timer := time.NewTimer(c.replica.RequestTimeout)
		for i := 0; i < len(s.batch); i++ {
			select {
			case <-timer.C:
				return errors.Wrapf(TimeoutError, "Timed out writing segment [%v]", c.replica.RequestTimeout)
			case <-c.ctrl.Closed():
				return ClosedError
			case data <- s.batch[i]:
			}
		}
		return nil
	}

	if err := streamSegment(segment); err != nil {
		req.Fail(err)
		return
	}

	req.Ack(newResponse(c.term.Num, true))
	for offset := len(segment.batch); ; {
		electionTimer := time.NewTimer(c.replica.ElectionTimeout)
		c.logger.Debug("Resetting election timeout: %v", c.replica.ElectionTimeout)

		select {
		case <-c.ctrl.Closed():
			return
		case <-electionTimer.C:
			c.logger.Info("Waited too long for snapshot.")
			return
		case req = <-c.replica.Replications:
			c.handleReplication(req)
			continue
		case req = <-c.replica.VoteRequests:
			c.handleRequestVote(req)
			continue
		case req = <-c.replica.Snapshots:
		}

		segment = req.Body().(installSnapshot)
		if segment.batchOffset != offset {
			req.Ack(newResponse(c.term.Num, false))
			return
		}

		c.logger.Debug("Handling snapshot segment: [%v,%v]", offset, offset + len(segment.batch))
		if err := streamSegment(segment); err != nil {
			req.Fail(err)
			return
		}

		offset += len(segment.batch)
		if offset < segment.size {
			req.Ack(newResponse(c.term.Num, true))
			continue
		}

		select {
		case r := <-resp.reply:
			req.Ack(r)
		case e := <-resp.err:
			req.Fail(e)
		case <-c.ctrl.Closed():
			req.Fail(ClosedError)
		}
		return
	}
}

func (c *follower) handleRequestVote(req stdRequest) {
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

func (c *follower) handleReplication(req stdRequest) {
	append := req.Body().(replicate)

	if append.term < c.term.Num {
		req.Ack(newResponse(c.term.Num, true))
		return
	}

	hint, _, err := c.replica.Log.Last()
	if err != nil {
		req.Fail(err)
	}

	// c.logger.Debug("Handling replication: %v", append)
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
		c.logger.Error("Consistency check failed. Responding with hint [%v]", hint)
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

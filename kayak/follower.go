package kayak

import (
	"time"

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
	// timeout := c.replica.RequestTimeout / 2
	//
	// err := c.proxyPool.SubmitTimeout(timeout, func() {
	// cl := c.clientPool.TakeTimeout(timeout)
	// if cl == nil {
	// update.Fail(common.NewTimeoutError(timeout, "Error retrieving connection from pool."))
	// return
	// }
	//
	// if e := newClient(cl).UpdateRoster(update.peer, update.join); e != nil {
	// c.clientPool.Fail(cl)
	// update.Fail(e)
	// } else {
	// c.clientPool.Return(cl)
	// update.Ack()
	// }
	// })
	// if err != nil {
	// update.Fail(err)
	// }
}

func (c *follower) handleInstallSnapshot(req stdRequest) {
	snapshot := req.Body().(installSnapshot)
	req.Reply(response{snapshot.term, false})
}

func (c *follower) handleRequestVote(req stdRequest) {
	vote := req.Body().(requestVote)

	c.logger.Debug("Handling request vote [%v]", vote)

	// FIXME: Lots of duplicates here....condense down

	// handle: previous term vote.  (immediately decline.)
	if vote.term < c.replica.term.Num {
		req.Reply(response{c.replica.term.Num, false})
		return
	}

	// handle: current term vote.  (accept if no vote and if candidate log is as long as ours)
	maxIndex, maxTerm, err := c.replica.Log.Last()
	if err != nil {
		req.Reply(response{c.replica.term.Num, false})
		return
	}

	c.logger.Debug("Current log max: %v", maxIndex)
	if vote.term == c.replica.term.Num {
		if c.replica.term.VotedFor == nil && vote.maxLogIndex >= maxIndex && vote.maxLogTerm >= maxTerm {
			c.logger.Debug("Voting for candidate [%v]", vote.id)
			req.Reply(response{c.replica.term.Num, true})
			c.replica.Term(c.replica.term.Num, nil, &vote.id) // correct?
			becomeFollower(c.replica)
			c.ctrl.Close()
			return
		}

		c.logger.Debug("Rejecting candidate vote [%v]", vote.id)
		req.Reply(response{c.replica.term.Num, false})
		becomeCandidate(c.replica)
		c.ctrl.Close()
		return
	}

	// handle: future term vote.  (move to new term.  only accept if candidate log is long enough)
	if vote.maxLogIndex >= maxIndex && vote.maxLogTerm >= maxTerm {
		c.logger.Debug("Voting for candidate [%v]", vote.id)
		req.Reply(response{vote.term, true})
		c.replica.Term(vote.term, nil, &vote.id)
		becomeFollower(c.replica)
		c.ctrl.Close()
		return
	}

	c.logger.Debug("Rejecting candidate vote [%v]", vote.id)
	req.Reply(response{vote.term, false})
	c.replica.Term(vote.term, nil, nil)
	becomeCandidate(c.replica)
	c.ctrl.Close()
}

func (c *follower) handleReplication(req stdRequest) {
	append := req.Body().(replicateEvents)

	if append.term < c.replica.term.Num {
		req.Reply(response{c.replica.term.Num, false})
		return
	}

	c.logger.Info("Handling replication: %v", append)
	if append.term > c.replica.term.Num || c.replica.term.Leader == nil {
		c.logger.Info("New leader detected [%v]", append.id)
		req.Reply(response{append.term, false})
		c.replica.Term(append.term, &append.id, &append.id)
		becomeFollower(c.replica)
		c.ctrl.Close()
		return
	}

	// if this is a heartbeat, bail out
	c.replica.Log.Commit(append.commit)
	if len(append.items) == 0 {
		req.Reply(response{append.term, true})
		return
	}

	c.logger.Debug("Handling replication: %v", append)

	// consistency check
	if ok, err := c.replica.Log.Assert(append.prevLogIndex, append.prevLogTerm); !ok || err != nil {
		c.logger.Error("Consistency check failed(%v)", err)

		// FIXME: This will cause anyone listening to head to
		// have to recreate state!
		req.Reply(response{append.term, false})
		return
	}

	// insert items.
	c.replica.Log.Truncate(append.prevLogIndex + 1)
	if err := c.replica.Log.Insert(append.items); err != nil {
		c.logger.Error("Error inserting batch: %v", err)
		req.Reply(response{append.term, false})
		return
	}

	req.Reply(response{append.term, true})
}

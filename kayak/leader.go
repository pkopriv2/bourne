package kayak

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
)

type leader struct {
	ctx        common.Context
	logger     common.Logger
	ctrl       common.Control
	syncer     *logSyncer
	proxyPool  concurrent.WorkPool
	appendPool concurrent.WorkPool
	term       term
	replica    *replica
}

func becomeLeader(replica *replica) {
	replica.Term(replica.CurrentTerm().Num, &replica.Id, &replica.Id)

	ctx := replica.Ctx.Sub("Leader(%v)", replica.CurrentTerm())
	ctx.Logger().Info("Becoming leader")

	l := &leader{
		ctx:        ctx,
		logger:     ctx.Logger(),
		ctrl:       ctx.Control(),
		syncer:     newLogSyncer(ctx, replica),
		proxyPool:  concurrent.NewWorkPool(10),
		appendPool: concurrent.NewWorkPool(10),
		term:       replica.CurrentTerm(),
		replica:    replica,
	}

	ctx.Control().Defer(func(error) {
		l.ctrl.Close()
		l.proxyPool.Close()
		l.appendPool.Close()
		l.syncer.Close()
	})

	l.start()
}

func (l *leader) start() {
	// Establish leadership
	l.broadcastHeartbeat()

	// Proxy routine.
	go func() {
		for {
			select {
			case <-l.ctrl.Closed():
				return
			case req := <-l.replica.RemoteAppends:
				l.handleRemoteAppend(req)
			}
		}
	}()

	// Roster routine
	go func() {
		for {
			select {
			case <-l.ctrl.Closed():
				return
			case req := <-l.replica.RosterUpdates:
				l.handleRosterUpdate(req)
			}
		}
	}()

	// Main routine
	go func() {
		defer l.ctrl.Close()
		for {
			timer := time.NewTimer(l.replica.ElectionTimeout / 5)
			// l.logger.Debug("Resetting timeout [%v]", l.replica.ElectionTimeout/5)

			select {
			case <-l.ctrl.Closed():
				return
			case req := <-l.replica.LocalAppends:
				l.handleLocalAppend(req)
			case req := <-l.replica.Snapshots:
				l.handleInstallSnapshot(req)
			case req := <-l.replica.Replications:
				l.handleReplication(req)
			case req := <-l.replica.VoteRequests:
				l.handleRequestVote(req)
			case <-timer.C:
				l.broadcastHeartbeat()
			case <-l.syncer.ctrl.Closed():
				l.logger.Error("Sync'er closed: %v", l.syncer.ctrl.Failure())
				becomeFollower(l.replica)
				return
			}
		}
	}()
}

// leaders do not accept snapshot installations
func (c *leader) handleInstallSnapshot(req *common.Request) {
	snapshot := req.Body().(installSnapshot)
	if snapshot.term <= c.term.Num {
		req.Ack(newResponse(c.term.Num, false))
		return
	}

	c.replica.Term(snapshot.term, &snapshot.leaderId, &snapshot.leaderId)
	req.Ack(newResponse(snapshot.term, false))
	becomeFollower(c.replica)
	c.ctrl.Close()
}

// leaders do not accept replication requests
func (c *leader) handleReplication(req *common.Request) {
	append := req.Body().(replicate)
	if append.term <= c.term.Num {
		req.Ack(newResponse(c.term.Num, false))
		return
	}

	c.replica.Term(append.term, &append.id, &append.id)
	req.Ack(newResponse(append.term, false))
	becomeFollower(c.replica)
	c.ctrl.Close()
}

func (c *leader) handleRemoteAppend(req *common.Request) {
	err := c.proxyPool.Submit(func() {
		req.Return(c.replica.LocalAppend(req.Body().(appendEvent)))
	})

	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting work to proxy pool."))
	}
}

func (c *leader) handleLocalAppend(req *common.Request) {
	err := c.appendPool.SubmitTimeout(1000*time.Millisecond, func() {
		req.Return(c.syncer.Append(req.Body().(appendEvent)))
	})

	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting work to append pool."))
	}
}

func (c *leader) handleRosterUpdate(req *common.Request) {
	update := req.Body().(rosterUpdate)

	all := c.replica.Cluster()
	if !update.join {
		all = delPeer(all, update.peer)

		c.logger.Info("Removing peer: %v", update.peer)
		if _, e := c.replica.Append(clusterBytes(all), Config); e != nil {
			req.Fail(e)
			return
		} else {
			req.Ack(true)
			return
		}
	}

	var err error
	defer func() {
		if err != nil {
			c.logger.Info("Error adding peer [%v].  Removing from sync'ers.", update.peer)
			c.syncer.handleRosterChange(delPeer(all, update.peer))
		}
	}()

	all = addPeer(all, update.peer)
	c.syncer.handleRosterChange(all)

	sync := c.syncer.Syncer(update.peer.Id)

	// _, err = sync.heartbeat()
	// if err != nil {
		// req.Fail(err)
		// return
	// }

	score, err := sync.score()
	if err != nil {
		req.Fail(err)
		if err != ClosedError {
			becomeFollower(c.replica)
			c.ctrl.Fail(err)
		}
		return
	}

	if score < 0 {
		req.Fail(errors.Wrapf(TimeoutError, "Unable to merge peer: %v", update.peer))
		return
	}

	if _, e := c.replica.Append(clusterBytes(all), Config); e != nil {
		req.Fail(e)
		return
	} else {
		req.Ack(true)
		return
	}
}

func (c *leader) handleRequestVote(req *common.Request) {
	vote := req.Body().(requestVote)
	c.logger.Debug("Handling request vote: %v", vote)

	// handle: previous or current term vote.  (immediately decline.  already leader)
	if vote.term <= c.term.Num {
		req.Ack(newResponse(c.term.Num, false))
		return
	}

	// handle: future term vote.  (move to new term.  only accept if candidate log is long enough)
	maxIndex, maxTerm, err := c.replica.Log.Last()
	if err != nil {
		req.Ack(newResponse(vote.term, false))
		return
	}

	if vote.maxLogIndex >= maxIndex && vote.maxLogTerm >= maxTerm {
		c.replica.Term(vote.term, nil, &vote.id)
		req.Ack(newResponse(vote.term, true))
	} else {
		c.replica.Term(vote.term, nil, nil)
		req.Ack(newResponse(vote.term, false))
	}

	becomeFollower(c.replica)
	c.ctrl.Close()
}


func (c *leader) broadcastHeartbeat() {
	ch := c.replica.Broadcast(func(cl *rpcClient) response {
		resp, err := cl.Replicate(newHeartBeat(c.replica.Id, c.term.Num, c.replica.Log.Committed()))
		if err != nil {
			return newResponse(c.term.Num, false)
		} else {
			return resp
		}
	})

	timer := time.NewTimer(c.replica.ElectionTimeout)
	for i := 0; i < c.replica.Majority()-1; {
		select {
		case <-c.ctrl.Closed():
			return
		case resp := <-ch:
			if resp.term > c.term.Num {
				c.replica.Term(resp.term, nil, c.term.VotedFor)
				becomeFollower(c.replica)
				c.ctrl.Close()
				return
			}

			i++
		case <-timer.C:
			c.logger.Error("Unable to retrieve enough heartbeat responses.")
			c.replica.Term(c.term.Num, nil, c.term.VotedFor)
			becomeFollower(c.replica)
			c.ctrl.Close()
			return
		}
	}
}

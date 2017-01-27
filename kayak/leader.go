package kayak

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
)

type leader struct {
	logger     common.Logger
	control    common.Control
	syncer     *logSyncer
	proxyPool  concurrent.WorkPool
	appendPool concurrent.WorkPool
	term       term
	replica    *replica
}

func becomeLeader(replica *replica) {
	replica.Term(replica.CurrentTerm().Num, &replica.Id, &replica.Id)

	logger := replica.Logger.Fmt("Leader(%v)", replica.CurrentTerm())
	logger.Info("Becoming leader")

	l := &leader{
		logger:     logger,
		control:    replica.Ctx.Control().Sub(),
		syncer:     newLogSyncer(logger, replica),
		proxyPool:  concurrent.NewWorkPool(10),
		appendPool: concurrent.NewWorkPool(10),
		term:       replica.CurrentTerm(),
		replica:    replica,
	}

	l.start()
}

func (l *leader) start() {
	// Establish leadership
	l.broadcastHeartbeat()

	// Close routine.
	go func() {
		select {
		case <-l.control.Closed():
		case <-l.replica.closed:
		}

		l.control.Close()
		l.proxyPool.Close()
		l.appendPool.Close()
		l.syncer.Close()
	}()

	// Proxy routine.
	go func() {
		for {
			select {
			case <-l.replica.closed:
				return
			case <-l.control.Closed():
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
			case <-l.replica.closed:
				return
			case <-l.control.Closed():
				return
			case req := <-l.replica.RosterUpdates:
				l.handleRosterUpdate(req)
			}
		}
	}()

	// Main routine
	go func() {
		defer l.control.Close()
		for {
			timer := time.NewTimer(l.replica.ElectionTimeout / 5)
			l.logger.Debug("Resetting timeout [%v]", l.replica.ElectionTimeout/5)

			select {
			case <-l.control.Closed():
				return
			case <-l.replica.closed:
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
			case <-l.syncer.control.Closed():
				l.logger.Error("Sync'er closed: %v", l.syncer.control.Failure())
				if fail := l.syncer.control.Failure(); fail == NotLeaderError {
					becomeFollower(l.replica)
					return
				}
				return
			}
		}
	}()
}

func (c *leader) handleRemoteAppend(req stdRequest) {
	append := req.Body().(appendEvent)

	err := c.proxyPool.Submit(func() {
		req.Return(c.replica.LocalAppend(append))
	})

	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting work to proxy pool."))
	}
}

func (c *leader) handleLocalAppend(req stdRequest) {
	append := req.Body().(appendEvent)

	err := c.appendPool.SubmitTimeout(1000*time.Millisecond, func() {
		req.Return(c.syncer.Append(append.Event, append.Source, append.Seq, append.Kind))
	})

	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting work to append pool."))
	}
}

func (c *leader) handleInstallSnapshot(req stdRequest) {
}

func (c *leader) handleRosterUpdate(req stdRequest) {
	// TODO: start log syncer before adding config change.
	update := req.Body().(rosterUpdate)

	all := c.replica.Cluster()
	if update.join {
		all = addPeer(all, update.peer)
		c.syncer.handleRosterChange(all)
	} else {
		all = delPeer(all, update.peer)
	}

	c.logger.Info("Setting cluster config [%v]", all)
	if _, e := c.replica.Append(clusterBytes(all), Config); e != nil {
		req.Fail(e)
	} else {
		req.Reply(true)
	}
}

func (c *leader) handleRequestVote(req stdRequest) {
	vote := req.Body().(requestVote)
	c.logger.Debug("Handling request vote: %v", vote)

	// handle: previous or current term vote.  (immediately decline.  already leader)
	if vote.term <= c.term.Num {
		req.Reply(response{c.term.Num, false})
		return
	}

	// handle: future term vote.  (move to new term.  only accept if candidate log is long enough)
	maxIndex, maxTerm, err := c.replica.Log.Last()
	if err != nil {
		req.Reply(response{vote.term, false})
		return
	}

	defer c.control.Close()

	if vote.maxLogIndex >= maxIndex && vote.maxLogTerm >= maxTerm {
		c.replica.Term(vote.term, nil, &vote.id)
		req.Reply(response{vote.term, true})
	} else {
		c.replica.Term(vote.term, nil, nil)
		req.Reply(response{vote.term, false})
	}

	becomeFollower(c.replica)
}

func (c *leader) handleReplication(req stdRequest) {
	append := req.Body().(replicateEvents)

	if append.term <= c.term.Num {
		req.Reply(response{c.term.Num, false})
		return
	}

	defer c.control.Close()
	c.replica.Term(append.term, &append.id, &append.id)
	req.Reply(response{append.term, false})
	becomeFollower(c.replica)
}

func (c *leader) broadcastHeartbeat() {
	ch := c.replica.Broadcast(func(cl *rpcClient) response {
		resp, err := cl.Replicate(c.replica.Id, c.term.Num, -1, -1, []LogItem{}, c.replica.Log.Committed())
		if err != nil {
			return response{c.term.Num, false}
		} else {
			return resp
		}
	})

	timer := time.NewTimer(c.replica.ElectionTimeout)
	for i := 0; i < c.replica.Majority()-1; {
		select {
		case <-c.control.Closed():
			return
		case resp := <-ch:
			if resp.term > c.term.Num {
				c.replica.Term(resp.term, nil, c.term.VotedFor)
				becomeFollower(c.replica)
				c.control.Close()
				return
			}

			i++
		case <-timer.C:
			c.replica.Term(c.term.Num, nil, c.term.VotedFor)
			becomeFollower(c.replica)
			c.control.Close()
			return
		}
	}
}

package kayak

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
)

type leader struct {
	logger     common.Logger
	syncer     *logSyncer
	proxyPool  concurrent.WorkPool
	appendPool concurrent.WorkPool
	term       term
	replica    *replica
	closed     chan struct{}
	closer     chan struct{}
}

func becomeLeader(replica *replica) {
	replica.Term(replica.CurrentTerm().Num, &replica.Id, &replica.Id)

	logger := replica.Logger.Fmt("Leader(%v)", replica.CurrentTerm())
	logger.Info("Becoming leader")

	closed := make(chan struct{})
	l := &leader{
		logger:     logger,
		syncer:     newLogSyncer(logger, replica),
		proxyPool:  concurrent.NewWorkPool(10),
		appendPool: concurrent.NewWorkPool(10),
		term:       replica.CurrentTerm(),
		replica:    replica,
		closed:     closed,
		closer:     make(chan struct{}, 1),
	}

	l.start()
}

func (l *leader) Close() error {
	select {
	case <-l.closed:
		return ClosedError
	case l.closer <- struct{}{}:
	}

	l.proxyPool.Close()
	l.appendPool.Close()
	l.syncer.Close()
	close(l.closed)
	return nil
}

func (l *leader) start() {
	// Establish leadership
	l.broadcastHeartbeat()

	// Proxy routine.
	go func() {
		for {
			select {
			case <-l.replica.closed:
				return
			case <-l.closed:
				return
			case append := <-l.replica.RemoteAppends:
				l.handleRemoteAppend(append)
			}
		}
	}()

	// Roster routine
	go func() {
		for {
			select {
			case <-l.replica.closed:
				return
			case <-l.closed:
				return
			case roster := <-l.replica.RosterUpdates:
				l.handleRosterUpdate(roster)
			}
		}
	}()

	// Main routine
	go func() {
		defer l.Close()
		for {
			timer := time.NewTimer(l.replica.ElectionTimeout / 5)
			l.logger.Debug("Resetting timeout [%v]", l.replica.ElectionTimeout/5)

			select {
			case <-l.closed:
				return
			case <-l.replica.closed:
				return
			case append := <-l.replica.LocalAppends:
				l.handleLocalAppend(append)
			case snapshot := <-l.replica.Snapshots:
				l.handleInstallSnapshot(snapshot)
			case events := <-l.replica.Replications:
				l.handleReplication(events)
			case ballot := <-l.replica.VoteRequests:
				l.handleRequestVote(ballot)
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

func (c *leader) handleRemoteAppend(append machineAppend) {
	err := c.proxyPool.Submit(func() {
		append.Return(c.replica.LocalAppend(append.Event, append.Source, append.Seq, append.Kind))
	})

	if err != nil {
		append.Fail(errors.Wrapf(err, "Error submitting work to proxy pool."))
	}
}

func (c *leader) handleLocalAppend(append machineAppend) {
	err := c.appendPool.SubmitTimeout(1000*time.Millisecond, func() {
		append.Return(c.syncer.Append(append.Event, append.Source, append.Seq, append.Kind))
	})

	if err != nil {
		append.Fail(errors.Wrapf(err, "Error submitting work to append pool."))
	}
}

func (c *leader) handleInstallSnapshot(snapshot installSnapshot) {
	snapshot.Reply(c.term.Num, false)
}

func (c *leader) handleRosterUpdate(update rosterUpdate) {
	// TODO: start log syncer before adding config change.

	all := c.replica.Cluster()
	if update.join {
		all = addPeer(all, update.peer)
	} else {
		all = delPeer(all, update.peer)
	}

	c.logger.Info("Setting cluster config [%v]", all)
	if _, e := c.replica.Append(clusterBytes(all), Config); e != nil {
		update.Fail(e)
	} else {
		update.Ack()
	}
}

func (c *leader) handleRequestVote(vote requestVote) {
	c.logger.Debug("Handling request vote: %v", vote)

	// handle: previous or current term vote.  (immediately decline.  already leader)
	if vote.term <= c.term.Num {
		vote.reply(c.term.Num, false)
		return
	}

	// handle: future term vote.  (move to new term.  only accept if candidate log is long enough)
	maxIndex, maxTerm, err := c.replica.Log.Last()
	if err != nil {
		vote.reply(vote.term, false)
		return
	}

	defer c.Close()

	if vote.maxLogIndex >= maxIndex && vote.maxLogTerm >= maxTerm {
		c.replica.Term(vote.term, nil, &vote.id)
		vote.reply(vote.term, true)
	} else {
		c.replica.Term(vote.term, nil, nil)
		vote.reply(vote.term, false)
	}

	becomeFollower(c.replica)
}

func (c *leader) handleReplication(append replicateEvents) {
	if append.term <= c.term.Num {
		append.reply(c.term.Num, false)
		return
	}

	defer c.Close()
	c.replica.Term(append.term, &append.id, &append.id)
	append.reply(append.term, false)
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
		case <-c.closed:
			return
		case resp := <-ch:
			if resp.term > c.term.Num {
				c.replica.Term(resp.term, nil, c.term.VotedFor)
				becomeFollower(c.replica)
				c.Close()
				return
			}

			i++
		case <-timer.C:
			c.replica.Term(c.term.Num, nil, c.term.VotedFor)
			becomeFollower(c.replica)
			c.Close()
			return
		}
	}
}

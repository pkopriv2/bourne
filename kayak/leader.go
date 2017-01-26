package kayak

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
)

type leaderSpawner struct {
	ctx      common.Context
	in       chan *replica
	follower chan<- *replica
	closed   chan struct{}
}

func newLeaderSpawner(ctx common.Context, in chan *replica, follower chan<- *replica, closed chan struct{}) *leaderSpawner {
	ret := &leaderSpawner{ctx, in, follower, closed}
	ret.start()
	return ret
}

func (c *leaderSpawner) start() {
	go func() {
		for {
			select {
			case <-c.closed:
				return
			case i := <-c.in:
				spawnLeader(c.follower, i)
			}
		}
	}()
}

type leader struct {
	logger     common.Logger
	follower   chan<- *replica
	syncer     *logSyncer
	proxyPool  concurrent.WorkPool
	appendPool concurrent.WorkPool
	term       term
	replica    *replica
	closed     chan struct{}
	closer     chan struct{}
}

func spawnLeader(follower chan<- *replica, replica *replica) {
	replica.Term(replica.CurrentTerm().Num, &replica.Id, &replica.Id)

	logger := replica.Logger.Fmt("Leader(%v)", replica.CurrentTerm())
	logger.Info("Becoming leader")

	closed := make(chan struct{})
	l := &leader{
		logger:     logger,
		follower:   follower,
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

func (l *leader) transition(ch chan<- *replica) {
	select {
	case <-l.closed:
	case ch <- l.replica:
	}

	l.Close()
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

	// Main routine
	go func() {
		for {
			timer := time.NewTimer(l.replica.ElectionTimeout / 5)
			l.logger.Debug("Resetting timeout [%v]", l.replica.ElectionTimeout/5)

			select {
			case <-l.closed:
				return
			case <-l.replica.closed:
				l.Close()
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
			case <-l.syncer.closed:
				l.logger.Error("Sync'er closed: %v", l.syncer.failure)
				if l.syncer.failure == NotLeaderError {
					l.transition(l.follower)
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

	if vote.maxLogIndex >= maxIndex && vote.maxLogTerm >= maxTerm {
		c.replica.Term(vote.term, nil, &vote.id)
		vote.reply(vote.term, true)
	} else {
		c.replica.Term(vote.term, nil, nil)
		vote.reply(vote.term, false)
	}

	c.transition(c.follower)
}

func (c *leader) handleReplication(append replicateEvents) {
	if append.term <= c.term.Num {
		append.reply(c.term.Num, false)
		return
	}

	c.replica.Term(append.term, &append.id, &append.id)
	append.reply(append.term, false)
	c.transition(c.follower)
}

func (c *leader) broadcastHeartbeat() {
	ch := c.replica.Broadcast(func(cl *client) response {
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
				c.transition(c.follower)
				return
			}

			i++
		case <-timer.C:
			c.replica.Term(c.term.Num, nil, c.term.VotedFor)
			c.transition(c.follower)
			return
		}
	}
}

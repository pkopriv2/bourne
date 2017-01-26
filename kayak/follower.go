package kayak

import (
	"fmt"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/net"
)

// Each spawner is responsible for accepting a repica and starting the
// the corresponding machine.
type followerSpawner struct {
	in        chan *replica
	candidate chan<- *replica
	closed    chan struct{}
}

func newFollowerSpawner(in chan *replica, candidate chan<- *replica, closed chan struct{}) *followerSpawner {
	ret := &followerSpawner{in, candidate, closed}
	ret.start()
	return ret
}

func (c *followerSpawner) start() {
	go func() {
		for {
			select {
			case <-c.closed:
				return
			case replica := <-c.in:
				spawnFollower(c.in, c.candidate, replica)
			}
		}
	}()
}

// The follower machine.  This
type follower struct {
	logger       common.Logger
	in           chan<- *replica
	candidate    chan<- *replica
	term         term
	replica      *replica
	proxyPool    concurrent.WorkPool
	snapshotPool concurrent.WorkPool
	appendPool   concurrent.WorkPool
	clientPool   net.ClientPool
	// connPool     net.ConnectionPool
	closed chan struct{}
	closer chan struct{}
}

func spawnFollower(in chan<- *replica, candidate chan<- *replica, replica *replica) {
	logger := replica.Logger.Fmt("Follower(%v)", replica.CurrentTerm())
	logger.Info("Becoming follower")

	conns := newLeaderConnectionPool(replica)
	var clientPool net.ClientPool
	if conns != nil {
		clientPool = net.NewClientPool(replica.Ctx, logger, conns)
	}

	l := &follower{
		logger:     logger,
		in:         in,
		candidate:  candidate,
		proxyPool:  concurrent.NewWorkPool(16),
		appendPool: concurrent.NewWorkPool(16),
		clientPool: clientPool,
		term:       replica.CurrentTerm(),
		replica:    replica,
		closed:     make(chan struct{}),
		closer:     make(chan struct{}, 1),
	}

	l.start()
}

func (l *follower) transition(ch chan<- *replica) {
	select {
	case <-l.closed:
	case ch <- l.replica:
	}

	l.Close()
}

func (l *follower) Close() error {
	select {
	case <-l.closed:
		return ClosedError
	case l.closer <- struct{}{}:
	}

	close(l.closed)
	l.proxyPool.Close()
	l.appendPool.Close()
	if l.clientPool != nil {
		l.clientPool.Close()
	}
	return nil
}

func (c *follower) start() {
	// Proxy routine. (out of band to prevent deadlocks between state machine and replicated log)
	if c.term.Leader != nil {
		go func() {
			for {
				select {
				case <-c.replica.closed:
					return
				case <-c.closed:
					return
				case append := <-c.replica.LocalAppends:
					c.handleLocalAppend(append)
				case append := <-c.replica.RemoteAppends:
					c.handleRemoteAppend(append)
				}
			}
		}()
	}

	// Main routine
	go func() {
		for {
			electionTimer := time.NewTimer(c.replica.ElectionTimeout)
			c.logger.Debug("Resetting election timeout: %v", c.replica.ElectionTimeout)

			select {
			case <-c.closed:
				return
			case <-c.replica.closed:
				c.Close()
				return
			case append := <-c.replica.Replications:
				c.handleReplication(append)
			case ballot := <-c.replica.VoteRequests:
				c.handleRequestVote(ballot)
			case snapshot := <-c.replica.Snapshots:
				c.handleInstallSnapshot(snapshot)
			case <-electionTimer.C:
				c.logger.Info("Waited too long for heartbeat.")
				c.transition(c.candidate)
				return
			}
		}
	}()
}

func (c *follower) handleLocalAppend(append machineAppend) {
	timeout := c.replica.RequestTimeout / 2

	err := c.proxyPool.SubmitTimeout(timeout, func() {
		cl := c.clientPool.TakeTimeout(timeout)
		if cl == nil {
			append.Fail(common.NewTimeoutError(timeout, "Error retrieving connection from pool."))
			return
		}

		i, e := newClient(cl).Append(append.Event, append.Source, append.Seq, append.Kind)
		if e == nil {
			c.clientPool.Return(cl)
		} else {
			c.clientPool.Fail(cl)
		}
		append.Return(i, e)
	})
	if err != nil {
		append.Fail(err)
	}
}

func (c *follower) handleRemoteAppend(append machineAppend) {
	append.Fail(NotLeaderError)
}

func (c *follower) handleInstallSnapshot(snapshot installSnapshot) {
	// handle: previous term vote.  (immediately decline.)
	if snapshot.term < c.replica.term.Num {
		snapshot.Reply(c.replica.term.Num, false)
		return
	}

	// storeDurableSnapshotSegment(snapshot.snapshotId, snapshot.batchOffset, snapshot.batch)
	snapshot.Reply(c.term.Num, false)
}

func (c *follower) handleRequestVote(vote requestVote) {
	c.logger.Debug("Handling request vote [%v]", vote)

	// handle: previous term vote.  (immediately decline.)
	if vote.term < c.replica.term.Num {
		vote.reply(c.replica.term.Num, false)
		return
	}

	// handle: current term vote.  (accept if no vote and if candidate log is as long as ours)
	maxIndex, maxTerm, err := c.replica.Log.Last()
	if err != nil {
		vote.reply(c.replica.term.Num, false)
		return
	}

	c.logger.Debug("Current log max: %v", maxIndex)
	if vote.term == c.replica.term.Num {
		if c.replica.term.VotedFor == nil && vote.maxLogIndex >= maxIndex && vote.maxLogTerm >= maxTerm {
			c.logger.Debug("Voting for candidate [%v]", vote.id)
			vote.reply(c.replica.term.Num, true)
			c.replica.Term(c.replica.term.Num, nil, &vote.id) // correct?
			c.transition(c.in)
			return
		}

		c.logger.Debug("Rejecting candidate vote [%v]", vote.id)
		vote.reply(c.replica.term.Num, false)
		c.transition(c.candidate)
		return
	}

	// handle: future term vote.  (move to new term.  only accept if candidate log is long enough)
	if vote.maxLogIndex >= maxIndex && vote.maxLogTerm >= maxTerm {
		c.logger.Debug("Voting for candidate [%v]", vote.id)
		vote.reply(vote.term, true)
		c.replica.Term(vote.term, nil, &vote.id)
		c.transition(c.in)
		return
	}

	c.logger.Debug("Rejecting candidate vote [%v]", vote.id)
	vote.reply(vote.term, false)
	c.replica.Term(vote.term, nil, nil)
	c.transition(c.candidate)
}

func (c *follower) handleReplication(append replicateEvents) {
	if append.term < c.replica.term.Num {
		append.reply(c.replica.term.Num, false)
		return
	}

	if append.term > c.replica.term.Num || c.replica.term.Leader == nil {
		c.logger.Info("New leader detected [%v]", append.id)
		append.reply(append.term, false)
		c.replica.Term(append.term, &append.id, &append.id)
		c.transition(c.in)
		return
	}

	// if this is a heartbeat, bail out
	c.replica.Log.Commit(append.commit)
	if len(append.items) == 0 {
		append.reply(append.term, true)
		return
	}

	c.logger.Debug("Handling replication: %v", append)

	// consistency check
	if ok, err := c.replica.Log.Assert(append.prevLogIndex, append.prevLogTerm); !ok || err != nil {
		c.logger.Error("Consistency check failed(%v)", err)

		// FIXME: This will cause anyone listening to head to
		// have to recreate state!
		c.replica.Log.Truncate(append.prevLogIndex + 1)
		append.reply(append.term, false)
		return
	}

	// insert items.
	if err := c.replica.Log.Insert(append.items); err != nil {
		c.logger.Error("Error inserting batch: %v", err)
		append.reply(append.term, false)
		return
	}

	append.reply(append.term, true)
}

func newLeaderConnectionPool(r *replica) net.ConnectionPool {
	if r.term.Leader == nil {
		return nil
	}

	leader, found := r.Peer(*r.term.Leader)
	if !found {
		panic(fmt.Sprintf("Unknown member [%v]: %v", r.term.Leader, r.Cluster()))
	}

	return leader.NewPool(r.Ctx)
}

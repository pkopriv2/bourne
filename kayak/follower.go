package kayak

import (
	"fmt"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/net"
)

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
	if c.term.leader != nil {
		go func() {
			for {
				select {
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

		i,e := newClient(cl).Append(append.Event)
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
	if snapshot.term < c.replica.term.num {
		snapshot.Reply(c.replica.term.num, false)
		return
	}

	// storeDurableSnapshotSegment(snapshot.snapshotId, snapshot.batchOffset, snapshot.batch)
	snapshot.Reply(c.term.num, false)
}

func (c *follower) handleRequestVote(vote requestVote) {
	c.logger.Debug("Handling request vote [%v]", vote)

	// handle: previous term vote.  (immediately decline.)
	if vote.term < c.replica.term.num {
		vote.reply(c.replica.term.num, false)
		return
	}

	// handle: current term vote.  (accept if no vote and if candidate log is as long as ours)
	max,err := c.replica.Log.Max()
	if err != nil {
		vote.reply(c.replica.term.num, false)
		return
	}

	c.logger.Debug("Current log max: %v", max)
	if vote.term == c.replica.term.num {
		if c.replica.term.votedFor == nil && vote.maxLogIndex >= max.Index && vote.maxLogTerm >= max.term {
			c.logger.Debug("Voting for candidate [%v]", vote.id)
			vote.reply(c.replica.term.num, true)
			c.replica.Term(c.replica.term.num, nil, &vote.id) // correct?
			c.transition(c.in)
			return
		}

		c.logger.Debug("Rejecting candidate vote [%v]", vote.id)
		vote.reply(c.replica.term.num, false)
		c.transition(c.candidate)
		return
	}

	// handle: future term vote.  (move to new term.  only accept if candidate log is long enough)
	if vote.maxLogIndex >= max.Index && vote.maxLogTerm >= max.term {
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
	if append.term < c.replica.term.num {
		append.reply(c.replica.term.num, false)
		return
	}

	if append.term > c.replica.term.num || c.replica.term.leader == nil {
		c.logger.Info("New leader detected [%v]", append.id)
		append.reply(append.term, false)
		c.replica.Term(append.term, &append.id, &append.id)
		c.transition(c.in)
		return
	}

	// if this is a heartbeat, bail out
	c.replica.Log.Commit(append.commit)
	if append.prevLogIndex == -1 && len(append.items) == 0 {
		append.reply(append.term, true)
		return
	}

	c.logger.Debug("Handling replication: %v", append)

	// consistency check
	if ok, err := c.replica.Log.Assert(append.prevLogIndex, append.prevLogTerm); !ok || err != nil {
		head := c.replica.Log.Head()
		act, _, _ := c.replica.Log.Get(append.prevLogIndex)
		prev := c.replica.Log.Active().raw.prevIndex

		c.logger.Error("Consistency check failed(%v): %v, Actual: %v, Head: %v, Prev: %v", err, append, act, head, prev)
		append.reply(append.term, false)
		return
	}

	// insert items.
	if _, err := c.replica.Log.Insert(append.items); err != nil {
		c.logger.Error("Error inserting batch: %v", err)
		append.reply(append.term, false)
		return
	}

	append.reply(append.term, true)
}

func newLeaderConnectionPool(r *replica) net.ConnectionPool {
	if r.term.leader == nil {
		return nil
	}

	leader, found := r.Peer(*r.term.leader)
	if !found {
		panic(fmt.Sprintf("Unknown member [%v]: %v", r.term.leader, r.Cluster()))
	}

	return net.NewConnectionPool("tcp", leader.addr, 10, 2*time.Second)
}

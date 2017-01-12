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
	logger     common.Logger
	in         chan<- *replica
	candidate  chan<- *replica
	proxyPool  concurrent.WorkPool
	appendPool concurrent.WorkPool
	connPool   net.ConnectionPool
	term       term
	replica    *replica
	closed     chan struct{}
	closer     chan struct{}
}

func spawnFollower(in chan<- *replica, candidate chan<- *replica, replica *replica) {
	logger := replica.Logger.Fmt("Follower(%v)", replica.CurrentTerm())
	logger.Info("Becoming follower")

	l := &follower{
		logger:     logger,
		in:         in,
		candidate:  candidate,
		proxyPool:  concurrent.NewWorkPool(16),
		appendPool: concurrent.NewWorkPool(16),
		connPool:   newLeaderConnectionPool(replica),
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
	if l.connPool != nil {
		l.connPool.Close()
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

	// FIXME: Remove once testing has improved
	if c.replica.Machine != nil {
		go func() {
			l, err := c.replica.Log.ListenCommits(0, 0)
			if err != nil {
				c.logger.Error("Unable to start commit listener: %+v", err)
				return
			}
			for {
				for i := 0; i<c.replica.SnapshotThreshold; i++ {
					select {
					case <-c.closed:
						return
					case <-l.Closed():
						return
					case <-l.Items():
						continue
					}
				}

				snapshot, index, err := c.replica.Machine.Snapshot()
				if err != nil {
					c.logger.Error("Error taking snapshot: %+v", err)
					continue
				}

				if err := c.replica.Log.Compact(snapshot, index); err != nil {
					c.logger.Error("Error compacting log: %+v", err)
					continue
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
			case <-electionTimer.C:
				c.logger.Info("Waited too long for heartbeat.")
				c.transition(c.candidate)
				return
			}
		}
	}()
}

func (c *follower) handleLocalAppend(append localAppend) {
	timeout := c.replica.RequestTimeout / 2

	err := c.proxyPool.SubmitTimeout(timeout, func() {
		conn := c.connPool.TakeTimeout(timeout)
		if conn == nil {
			append.Fail(common.NewTimeoutError(timeout, "Error retrieving connection from pool."))
			return
		}
		defer conn.Close()

		raw, err := net.NewClient(c.replica.Ctx, c.replica.Logger, conn)
		if err != nil {
			append.Fail(err)
			return
		}

		append.Return(newClient(raw, c.replica.Parser).Append(append.Event))
	})
	if err != nil {
		append.Fail(err)
	}

}

func (c *follower) handleRemoteAppend(append localAppend) {
	append.Fail(NotLeaderError)
}

func (c *follower) handleRequestVote(vote requestVote) {
	c.logger.Debug("Handling request vote [%v]", vote)

	// handle: previous term vote.  (immediately decline.)
	if vote.term < c.replica.term.num {
		vote.reply(c.replica.term.num, false)
		return
	}

	// handle: current term vote.  (accept if no vote and if candidate log is as long as ours)
	maxLogIndex, maxLogTerm, _ := c.replica.Log.Snapshot()
	if vote.term == c.replica.term.num {
		if c.replica.term.votedFor == nil && vote.maxLogIndex >= maxLogIndex && vote.maxLogTerm >= maxLogTerm {
			vote.reply(c.replica.term.num, true)
			c.replica.Term(c.replica.term.num, nil, &vote.id) // correct?
			c.transition(c.in)
			return
		}

		vote.reply(c.replica.term.num, false)
		return
	}

	// handle: future term vote.  (move to new term.  only accept if candidate log is long enough)
	if vote.maxLogIndex >= maxLogIndex && vote.maxLogTerm >= maxLogTerm {
		vote.reply(vote.term, true)
		c.replica.Term(vote.term, nil, &vote.id)
		c.transition(c.in)
		return
	}

	vote.reply(vote.term, false)
	c.replica.Term(vote.term, nil, nil)
	c.transition(c.in)
}

func (c *follower) handleReplication(append replicateEvents) {
	c.logger.Debug("Handling append events: %v", append)
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

	if logItem, ok := c.replica.Log.Get(append.prevLogIndex); ok && logItem.term != append.prevLogTerm {
		c.logger.Info("Inconsistent log detected [%v,%v]. Rolling back", logItem.term, append.prevLogTerm)
		append.reply(append.term, false)
		return
	}

	c.replica.Log.Insert(append.events, append.prevLogIndex+1, append.term)
	c.replica.Log.Commit(append.commit)
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

	return net.NewConnectionPool("tcp", leader.addr, 30, r.ElectionTimeout)
}

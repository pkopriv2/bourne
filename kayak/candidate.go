package kayak

import (
	"time"

	"github.com/pkopriv2/bourne/common"
)

type candidateSpawnwer struct {
	ctx      common.Context
	in       chan *replica
	leader   chan<- *replica
	follower chan<- *replica
	closed   chan struct{}
}

func newCandidateSpawner(ctx common.Context, in chan *replica, leader chan<- *replica, follower chan<- *replica, closed chan struct{}) *candidateSpawnwer {
	ret := &candidateSpawnwer{ctx, in, leader, follower, closed}
	ret.start()
	return ret
}

func (c *candidateSpawnwer) start() {
	go func() {
		for {
			select {
			case <-c.closed:
				return
			case i := <-c.in:
				spawnCandidate(c.in, c.leader, c.follower, i)
			}
		}
	}()
}

type candidate struct {
	logger   common.Logger
	in       chan<- *replica
	leader   chan<- *replica
	follower chan<- *replica

	term    term
	replica *replica

	closed chan struct{}
	closer chan struct{}
}

func spawnCandidate(in chan<- *replica, leader chan<- *replica, follower chan<- *replica, replica *replica) {
	// increment term and vote for self.
	replica.Term(replica.term.num+1, nil, &replica.Id)

	logger := replica.Logger.Fmt("Candidate(%v)", replica.CurrentTerm())
	logger.Info("Becoming candidate")

	l := &candidate{
		logger:   logger,
		in:       in,
		leader:   leader,
		follower: follower,
		term:     replica.CurrentTerm(),
		replica:  replica,
		closed:   make(chan struct{}),
		closer:   make(chan struct{}, 1),
	}

	l.start()
}

func (c *candidate) transition(ch chan<- *replica) {
	select {
	case <-c.closed:
	case ch <- c.replica:
	}

	c.Close()
}

func (c *candidate) Close() error {
	select {
	case <-c.closed:
		return ClosedError
	case c.closer <- struct{}{}:
	}

	close(c.closed)
	return nil
}

func (c *candidate) start() {

	max, err := c.replica.Log.Max()
	ballots := c.replica.Broadcast(func(cl *client) response {
		c.logger.Debug("Sending ballots: %v", max)
		if err != nil {
			return response{c.replica.term.num, false}
		}

		resp, err := cl.RequestVote(c.replica.Id, c.replica.term.num, max.Index, max.term)
		if err != nil {
			return response{c.replica.term.num, false}
		} else {
			return resp
		}
	})

	go func() {
		// set the election timer.
		c.logger.Info("Setting timer [%v]", c.replica.ElectionTimeout)
		timer := time.NewTimer(c.replica.ElectionTimeout)

		for numVotes := 1; ; {
			c.logger.Info("Received [%v/%v] votes", numVotes, len(c.replica.Cluster()))

			needed := c.replica.Majority()
			if numVotes >= needed {
				c.logger.Info("Acquired majority [%v] votes.", needed)
				c.replica.Term(c.replica.term.num, &c.replica.Id, &c.replica.Id)
				c.transition(c.leader)
				return
			}

			select {
			case <-c.closed:
				return
			case append := <-c.replica.Replications:
				c.handleAppendEvents(append)
			case ballot := <-c.replica.VoteRequests:
				c.handleRequestVote(ballot)
			case <-timer.C:
				c.logger.Info("Unable to acquire necessary votes [%v/%v]", numVotes, needed)
				timer := time.NewTimer(c.replica.ElectionTimeout)
				select {
				case <-c.closed:
					return
				case <-timer.C:
					c.transition(c.in) // becomes a new candidate
					return
				}
			case vote := <-ballots:
				if vote.term > c.term.num {
					c.replica.Term(vote.term, nil, nil)
					c.transition(c.follower)
					return
				}

				if vote.success {
					numVotes++
				}
			}
		}
	}()
}

func (c *candidate) handleRequestVote(vote requestVote) {
	c.logger.Debug("Handling request vote: %v", vote)
	if vote.term <= c.term.num {
		vote.reply(c.term.num, false)
		return
	}

	max, err := c.replica.Log.Max()
	if err != nil {
		vote.reply(c.replica.term.num, false)
		return
	}

	if vote.maxLogIndex >= max.Index && vote.maxLogTerm >= max.term {
		c.logger.Debug("Voting for candidate [%v]", vote.id.String()[:8])
		vote.reply(vote.term, true)
		c.replica.Term(vote.term, nil, &vote.id)
		c.transition(c.follower)
		return
	}

	c.logger.Debug("Rejecting candidate vote [%v]", vote.id.String()[:8])
	vote.reply(vote.term, false)
	c.replica.Term(vote.term, nil, nil)
}

func (c *candidate) handleAppendEvents(append replicateEvents) {
	if append.term < c.term.num {
		append.reply(c.term.num, false)
		return
	}

	// append.term is >= term.  use it from now on.
	append.reply(append.term, false)
	c.replica.Term(append.term, &append.id, &append.id)
	c.transition(c.follower)
}

package kayak

import (
	"time"

	"github.com/pkopriv2/bourne/common"
)

type candidate struct {
	logger  common.Logger
	term    term
	replica *replica
	closed  chan struct{}
	closer  chan struct{}
}

func becomeCandidate(replica *replica) {
	// increment term and vote for self.
	replica.Term(replica.term.Num+1, nil, &replica.Id)

	logger := replica.Logger.Fmt("Candidate(%v)", replica.CurrentTerm())
	logger.Info("Becoming candidate")

	l := &candidate{
		logger:  logger,
		term:    replica.CurrentTerm(),
		replica: replica,
		closed:  make(chan struct{}),
		closer:  make(chan struct{}, 1),
	}

	l.start()
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

	maxIndex, maxTerm, err := c.replica.Log.Last()
	ballots := c.replica.Broadcast(func(cl *rpcClient) response {
		c.logger.Debug("Sending ballots: %v", maxIndex)
		if err != nil {
			return response{c.replica.term.Num, false}
		}

		resp, err := cl.RequestVote(c.replica.Id, c.replica.term.Num, maxIndex, maxTerm)
		if err != nil {
			return response{c.replica.term.Num, false}
		} else {
			return resp
		}
	})

	go func() {
		defer c.Close()

		// set the election timer.
		c.logger.Info("Setting timer [%v]", c.replica.ElectionTimeout)
		timer := time.NewTimer(c.replica.ElectionTimeout)

		for numVotes := 1; ; {
			c.logger.Info("Received [%v/%v] votes", numVotes, len(c.replica.Cluster()))

			needed := c.replica.Majority()
			if numVotes >= needed {
				c.logger.Info("Acquired majority [%v] votes.", needed)
				c.replica.Term(c.replica.term.Num, &c.replica.Id, &c.replica.Id)
				becomeLeader(c.replica)
				c.Close()
				return
			}

			select {
			case <-c.closed:
				return
			case <-c.replica.closed:
				return
			case req := <-c.replica.Replications:
				c.handleAppendEvents(req)
			case req := <-c.replica.VoteRequests:
				c.handleRequestVote(req)
			case <-timer.C:
				c.logger.Info("Unable to acquire necessary votes [%v/%v]", numVotes, needed)
				timer := time.NewTimer(c.replica.ElectionTimeout)
				select {
				case <-c.closed:
					return
				case <-timer.C:
					becomeCandidate(c.replica)
					return
				}
			case vote := <-ballots:
				if vote.term > c.term.Num {
					c.replica.Term(vote.term, nil, nil)
					becomeFollower(c.replica)
					return
				}

				if vote.success {
					numVotes++
				}
			}
		}
	}()
}

func (c *candidate) handleRequestVote(req stdRequest) {
	vote := req.Body().(requestVote)

	c.logger.Debug("Handling stdRequest vote: %v", vote)
	if vote.term <= c.term.Num {
		req.Reply(response{c.term.Num, false})
		return
	}

	maxIndex, maxTerm, err := c.replica.Log.Last()
	if err != nil {
		req.Reply(response{c.replica.term.Num, false})
		return
	}

	if vote.maxLogIndex >= maxIndex && vote.maxLogTerm >= maxTerm {
		c.logger.Debug("Voting for candidate [%v]", vote.id.String()[:8])
		req.Reply(response{vote.term, true})
		c.replica.Term(vote.term, nil, &vote.id)
		becomeFollower(c.replica)
		c.Close()
		return
	}

	c.logger.Debug("Rejecting candidate vote [%v]", vote.id.String()[:8])
	req.Reply(response{vote.term, false})
	c.replica.Term(vote.term, nil, nil)
}

func (c *candidate) handleAppendEvents(req stdRequest) {
	append := req.Body().(replicateEvents)

	if append.term < c.term.Num {
		req.Reply(response{c.term.Num, false})
		return
	}

	// append.term is >= term.  use it from now on.
	req.Reply(response{c.term.Num, false})
	c.replica.Term(append.term, &append.id, &append.id)
	becomeFollower(c.replica)
	c.Close()
}

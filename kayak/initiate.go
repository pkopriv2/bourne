package kayak

import (
	"github.com/pkopriv2/bourne/common"
)

type initiate struct {
	logger  common.Logger
	control common.Control
	replica *replica
}

func becomeInitiate(replica *replica) {
	logger := replica.Logger.Fmt("Intiate(%v)", replica.CurrentTerm())
	logger.Info("Becoming initiate")

	l := &initiate{
		logger:  logger,
		replica: replica,
		control: replica.Ctx.Control().Sub(),
	}

	l.start()
}

func (l *initiate) start() {
	// Main routine
	go func() {
		defer l.control.Close()
		defer l.logger.Info("Shutting down.")

		for {
			select {
			case <-l.replica.closed:
				return
			case <-l.control.Closed():
				return
			case req := <-l.replica.Replications:
				l.handleReplication(req)
			case req := <-l.replica.VoteRequests:
				l.handleRequestVote(req)
			}
		}
	}()
}

func (c *initiate) handleRequestVote(req stdRequest) {
	vote := req.Body().(requestVote)
	req.Reply(response{vote.term, true})
	c.replica.Term(vote.term, nil, nil)
	becomeFollower(c.replica)
	c.control.Close()
}

func (c *initiate) handleReplication(req stdRequest) {
	append := req.Body().(replicateEvents)
	req.Reply(response{append.term, false})
	c.replica.Term(append.term, &append.id, &append.id)
	becomeFollower(c.replica)
	c.control.Close()
}

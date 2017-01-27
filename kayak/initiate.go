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
		control: replica.Ctx.Control().Child(),
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
			case events := <-l.replica.Replications:
				l.handleReplication(events)
			case ballot := <-l.replica.VoteRequests:
				l.handleRequestVote(ballot)
			}
		}
	}()
}

func (c *initiate) handleRosterUpdate(update rosterUpdate) {
	update.Fail(NotLeaderError)
}

func (c *initiate) handleRequestVote(vote requestVote) {
	vote.reply(vote.term, false)
}

func (c *initiate) handleReplication(append replicateEvents) {
	append.reply(append.term, false)
	c.replica.Term(append.term, &append.id, &append.id)
	becomeFollower(c.replica)
	c.control.Close()
}

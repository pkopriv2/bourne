package kayak

import (
	"github.com/pkopriv2/bourne/common"
)

type initiate struct {
	logger  common.Logger
	replica *replica
	closed  chan struct{}
	closer  chan struct{}
}

func becomeInitiate(replica *replica) {
	logger := replica.Logger.Fmt("Intiate(%v)", replica.CurrentTerm())
	logger.Info("Becoming initiate")

	l := &initiate{
		logger:  logger,
		replica: replica,
		closed:  make(chan struct{}),
		closer:  make(chan struct{}, 1),
	}

	l.start()
}

func (l *initiate) Close() error {
	select {
	case <-l.closed:
		return ClosedError
	case l.closer <- struct{}{}:
	}

	close(l.closed)
	return nil
}

func (l *initiate) start() {
	// Main routine
	go func() {
		defer l.Close()

		for {
			select {
			case <-l.closed:
				return
			case <-l.replica.closed:
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
}

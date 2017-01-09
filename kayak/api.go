package kayak

import (
	"errors"

	"github.com/pkopriv2/bourne/scribe"
)

// Public Error Types
var (
	EvictedError   = errors.New("Kayak:Evicted")
	FailedError    = errors.New("Kayak:Failed")
	ClosedError    = errors.New("Kayak:Closed")
	NotLeaderError = errors.New("Kayak:NotLeader")
	NoLeaderError  = errors.New("Kayak:NoLeader")
)

type Parser func(scribe.Reader) (Event, error)

type Event interface {
	scribe.Writable
}

type StateMachine interface {
	// Every state machine must be expressable as a sequence of events.
	Snapshot() []Event

	// Handles an event, which is a "request" to update the machine.
	// Returns false if the event has no effect and should be ignored.
	Handle(Event) bool
}

type ReplicatedLog interface {
	// Returns a channel that
	Commits() <-chan Event

	// Appends to the lock.  Returns false, if the event has been rejected.
	Append(Event) (bool, error)
}

func NewReplicatedLog(StateMachine) (ReplicatedLog, error) {
	return nil, nil
}

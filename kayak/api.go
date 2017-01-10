package kayak

import (
	"errors"
	"io"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
)

// Public Error Types
var (
	EvictedError   = errors.New("Kayak:Evicted")
	FailedError    = errors.New("Kayak:Failed")
	ClosedError    = errors.New("Kayak:Closed")
	NotLeaderError = errors.New("Kayak:NotLeader")
	NoLeaderError  = errors.New("Kayak:NoLeader")
	EventError     = errors.New("Kayak:EventError")
)

type Parser func(scribe.Reader) (Event, error)

type Event interface {
	scribe.Writable
}

// A machine is anything that is expressable as a sequence of events.
//
// This is the primary consumer abstraction
type Machine interface {

	// The context used to create this machine.
	Context() common.Context

	// Used to parse events as they are received from other replicas.
	Parser() Parser

	// Every state machine must be expressable as a sequence of events.
	// The snapshot should be the minimal number of events that are
	// required such that:
	//
	// ```go
	// copy := NewStateMachine(...)
	// for _, e := range machine.Snapshot() {
	//    copy.Handle(e)
	// }
	// ```
	//
	// Results in the same machine state.
	Snapshot() ([]Event, error)

	// Runs the main machine routine.
	Run(log MachineLog)

	// Runs the proxy instance of the machine.
	RunProxy(<-chan AppendRequest)
}

type MachineLog interface {
	io.Closer

	// channel immediately returns values when closed.
	Closed() <-chan struct{}

	// // Returns a flag indicating whether this log is the master log.
	// IsMaster() bool

	// Returns the items in the log as they have been fully replicated. These
	// are guaranteed to return in the order they were appended without
	// jumps in the logs.
	Committed() <-chan LogItem

	// Appends the event to the log.
	//
	// !IMPORTANT!
	// If the append is successful, Do not assume that all committed items
	// have been replicated to this instance.  To faciliate the
	Append(Event) (bool, LogItem, error)
}

type LogItem struct {
	Event Event
	Index int
}

func Replicate(machine Machine, self string, peers []string) error {
	return nil
}

func NewAppendRequest(e Event) *AppendRequest {
	return &AppendRequest{e, make(chan AppendResponse)}
}

type AppendRequest struct {
	Event Event
	ack   chan AppendResponse
}

func (a *AppendRequest) Reject() {
	a.ack <- AppendResponse{false, 0, nil}
}

func (a *AppendRequest) Fail(err error) {
	a.ack <- AppendResponse{false, 0, err}
}

func (a *AppendRequest) committed(index int) {
	a.ack <- AppendResponse{true, index, nil}
}

type AppendResponse struct {
	Committed bool
	Index     int
	Error     error
}

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
// However, this machine has the special behavior that it may host
// an optional proxy server.  In raft parlance, this is akin to
// being the leader, however, the consumer only sees it as two
// components, one that is responsible for handling committed
// state changes.
//
// Although not strictly required, a typical state machine design
// will include at least two primary routines:
//
//   * The main routine.  (may be split further as desired)
//   * The proxy routine.  Only active when the local instance
//     happens to also be the leader.
//
// A typical signaling design may look like:
//
//                    |------*commits*-----|
//                    v                    |
// {consumer} ----->{Main}---*append*--->{Log}<------>{Peer}
//
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

	// Commits the log item to the machine. This is guaranteed
	// to be called sequentially for every log item that is appended
	// the log.
	Commit(LogItem)
}

// The machine log is a replicated log.
type MachineLog interface {
	io.Closer

	// Control channel that returns immediately once the
	// log has been closed.
	Closed() <-chan struct{}

	// Appends the event to the log.
	//
	// !IMPORTANT!
	// If the append is successful, Do not assume that all committed items
	// have been replicated to this instance.  Appends should always accompany
	// a sync routine that ensures that the log has been caught up prior to
	// returning control.
	Append(Event) (LogItem, error)
}

type LogItem struct {
	Event Event
	Index int
}

func Run(machine Machine, self string, peers []string) error {
	return nil
}

// type AppendRequest struct {
	// Event Event
	// ack   chan AppendResponse
// }
//
// func (a *AppendRequest) Reply(i int, s bool, e error) {
	// a.ack <- AppendResponse{i, s, e}
// }
//
// type AppendResponse struct {
	// Index   int
	// Success bool
	// Error   error
// }

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
// Multiple machines may be run as a group, to form a replicated
// machine.  Each machine will have access to a distributed log.
// It is very important to remember that each instance of the machine
// is interacting with all others, in order to maintain a consistent
// view of the log, and moreover, each instance may have to reconcile
// it's view with a peer.  The focus of this api is on creating
// applications who favor correctness over performance.
//
// # Distributed Consensus
//
// Underpinning every machine is a log that implements the Raft,
// distributed consensus protocol.  While most of the details of the
// protocol are abstracted away, it is useful to know some of the
// high level details.
//
// At startup, the machines are aware of each other's existence
// and form a fully-connected graph between them.  The machines
// use the raft leader election protocol to establish a leader
// amongst the group and the leader maintains its leadership through
// the use of periodic heartbeats.
//
// If a leader dies or becomes unreachable, the previous followers
// will hold an election to determine a new leader.  If the previous
// leader is able to re-establish a connection with the group, it
// detects that it has been usurped and becomes a follower.  The
// protocol also specifies how to bring the former leader back into
// a consistent state.
//
// Perhaps the most important aspect of Raft is its simplicity with
// respect to data flow design.
//
// * Updates flow from leader to follower. Period.
//
// And it turns out that this one property can make the management
// of a distributed log a very tractable problem, even when exposed
// directly to consumer machines.
//
// # Building State Machines
//
// Now that we've got some of the basics of distributed consensus out of
// the way, we can start looking at some basic machine designs. The
// machine and log interactions look like the following:
//
//
//                               |-------*commits*-----|
//                               v                     |
// {consumer} ---*updates*-->{Machine}---*append*--->{Log}<------>{Peer}
//
//
// And now we've got to the first issue of machine design:
//
// * Appends and commits are separate, unsynchronized streams.
//
// Moreover, users of these apis CANNOT make any assumptions about the
// relationship of one stream to the other.  In other words, a successful
// append ONLY gives the guarantee that it has been committed to a majority
//
// TODO: Is this necessary?  Can the log guarantee that an append only
// returns once it has been replicated to this instance?
//
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

	// Commits the log item to the machine. This is guaranteed to be
	// called sequentially for every log item that is appended and
	// subsequently committed (i.e. received by a majority) to the log.
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

type Listener interface {
	io.Closer
	Next() LogItem
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

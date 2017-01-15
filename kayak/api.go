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
	CanceledError  = errors.New("Kayak:Canceled")
	NotLeaderError = errors.New("Kayak:NotLeader")
	NoLeaderError  = errors.New("Kayak:NoLeader")
	StateError     = errors.New("Kayak:StateError")
	EventError     = errors.New("Kayak:EventError")
)

// An event is just a byte slice.  Interpretation of the bytes are the
// responsibility of the consumer.
type Event []byte

func (e Event) Raw() []byte {
	return []byte(e)
}

func (e Event) Write(w scribe.Writer) {
	w.WriteBytes("raw", []byte(e))
}

func EventParser(r scribe.Reader) (interface{}, error) {
	var ret []byte
	err := r.ReadBytes("raw", &ret)
	return Event(ret), err
}


// A machine is anything that is expressable as a sequence of events.
//
// Kayak allows consumers to utilize a generic, replicated event
// log in order to create highly-resilient, strongly consistent
// replicated state machines.
//
// Kayak machines work by using a distributed systems technique,
// called distributed consensus.
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
//                  *Local Process*						      |  *Network Process*
//                                                            |
//                               |-------*commits*-----|      |
//                               v                     |      |
// {Consumer}---*updates*--->{Machine}---*append*--->{Log}<---+--->{Peer}
//                                                            |
//                                                            |
//
//
// And now we've got to the first issue of machine design:
//
// * Appends and commits are separate, unsynchronized streams.
//
// Moreover, users of these apis CANNOT make any assumptions about the
// relationship of one stream to the other.  In other words, a successful
// append ONLY gives the guarantee that it has been committed to a majority
// of peers, and not necessarily to itself.  If consumers require strict
// linearizable reads, they are encouraged to sync their append request
// with the commit stream.
//
// TODO: Is this necessary?  Can the log guarantee that an append only
// returns once it has been replicated to this instance?
//
// References:
// * Original Lamport Paxos paper:
//     https://www.microsoft.com/en-us/research/wp-content/uploads/2016/12/The-Part-Time-Parliament.pdf
//
type Machine interface {

	// The context used to create this machine.
	Context() common.Context

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
	Snapshot() (snapshot []Event, maxIndex int, err error)

	// Runs the main machine routine.  This may be called many
	// times throughout the lifetime of the machine.
	//
	// The provided log will
	Run(log Log) error
}

// The log is the view into the replicated log state.  This allows
// consumer the ability to append events to the log and watch the
// log for changes.
type Log interface {
	io.Closer
//
	// // Returns all the machine log's items (Useful for backfilling state)
	// Scan(start int, end int) ([]LogItem, error)

	// Adds a real-time listener to the log commits.  The listener is guaranteed
	// to receive ALL items in the order they are committed - however -
	// the listener does NOT return all historical items.
	//
	// Please use #All() for backfilling.
	Listen(from int, buf int) (Listener, error)

	// Appends the event to the log.
	//
	// !IMPORTANT!
	// If the append is successful, Do not assume that all committed items
	// have been replicated to this instance.  Appends should always accompany
	// a sync routine that ensures that the log has been caught up prior to
	// returning control.
	Append(Event) (LogItem, error)
}

// The log listener.
type Listener interface {
	io.Closer

	// Returns a channel that returns items as they are passed.
	Items() <-chan LogItem

	// Returns a channel that immeditely returns when the listener
	// is closed
	Closed() <-chan struct{}
}

type LogItem struct {
	Index int
	Event Event

	// Internal:
	term int
	// config bool
}

func (l LogItem) Write(w scribe.Writer) {
	w.WriteInt("index", l.Index)
	w.WriteInt("term", l.term)
	w.WriteBytes("event", l.Event.Raw())
}

func readLogItem(r scribe.Reader) (interface{}, error) {
	var err error
	var item LogItem
	err = common.Or(err, r.ReadInt("index", &item.Index))
	err = common.Or(err, r.ReadInt("term", &item.term))
	err = common.Or(err, r.ParseMessage("event", &item.Event, EventParser))
	return item, err
}

func newEventLogItem(i int, t int, e Event) LogItem {
	return LogItem{Index: i, term: t, Event: e}
}

func Replicate(machine Machine, self string, peers []string) error {
	return nil
}

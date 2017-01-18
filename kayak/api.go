package kayak

import (
	"errors"
	"io"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
)

// Public Error Types
var (
	ClosedError        = errors.New("Kayak:Closed")
	NotLeaderError     = errors.New("Kayak:NotLeader")
	NoLeaderError      = errors.New("Kayak:NoLeader")
)

// Runs the machine, using a replicated event log as the durable storage
// engine.  Machines of these types can continue to function as long
// as a majority of peers remain alive.  If the remaining members are
// unable to form a quorum, client requests will be returned with a
// NoLeaderError
//
func Run(ctx common.Context, app Machine, self string, peers []string) error {
	return nil
}

// Events are the fundamental unit of replication.  This the primary
// consumer data structure used in interacting with the replicated log.
// The onus of interpretting an event is soley a consumer responsibility.
type Event []byte

func (e Event) Raw() []byte {
	return []byte(e)
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
// # Log Compactions
//
// For many machines, their state  is actually represented by many redundant
// log items.  In other words, many of the items have been obviated.  The log
// can leverage this to occasionally shrink the log.
//
// TODO: Is this necessary?  Can the log guarantee that an append only
// returns once it has been replicated to this instance?
//
// References:
// * Reft Spec:
//     https://raft.github.io/raft.pdf
// * Raft Book:
//
// * Original Lamport Paxos paper:
//     https://www.microsoft.com/en-us/research/wp-content/uploads/2016/12/The-Part-Time-Parliament.pdf
//
type Machine interface {

	// // Snapshot() (num int, stream <-chan Event, lastIncluded int, err error)

	// Runs the main machine routine.  This may be called many
	// times throughout the lifetime of the machine, therefore,
	// care needs to be taken to maintain consistency between
	// the machine state and the snapshot.
	//
	Run(log Log) error
}

// The log is the view into the replicated log state.  This allows
// consumer the ability to append events to the log and watch the
// log for changes.
type Log interface {
	io.Closer

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
	//
	// In order to be space efficient and prevent the underlying
	// log from growing without bounds, the log occasionally needs to be
	// cleaned by a process known as log compaction.  Essentially, the log
	// until a certain point will be replaced with a *hopefully* smaller snapshot.
	// However, it cannot do this with some help from the state machine.
	// Machines must provide a mechanism by which the log manager a compacted
	// view of its log.  Consumers do so by returning a stream of events  that
	// represent its state up until the index in the log.   All log items and
	// their events will be discarded up until the index.
	//
	//
	// NOTE: I originally struggled with how to model an event stream,
	// but realized that channels fit pretty naturally.  I tend to
	// avoid channel based apis, but this one seems simple enough
	// for our needs.
	Compact(until int, stream <-chan Event, size int) error

}

// The log listener.
type Listener interface {
	io.Closer

	// Returns the next log item in the listener.  If an error is returned,
	// it may be for a variety of reasons.  Here are a couple:
	//
	// * The underlying log has been closed.
	// * The underlying log has been compacted away.  (*Important: See Below)
	// * There was a system/disc error.
	//
	// If a reader gets significantly behind the underlying log's end, it
	// is possible for the listener to become corrupted if the underlying log
	// is compacted away (See: Machine#Snapshot()). Consumers are allowed to
	// choose how to deal with this, but for in-memory state machines, if the
	// the stream was in critical path to machine state, then it's probably best
	// to just rebuild the machine.
	//
	// The possible error values are:
	//
	// * EventError
	// * StorageError
	// * SlowConsumerError
	//
	Next() (LogItem, error)
}

// The basic log item.  This is typically just an event decorated with its index
// in the log.
type LogItem struct {

	// Item index. May be used to reconcile state with log.
	Index int

	// The event bytes.  This is the fundamental unit of replication.
	Event Event

	// Internal Only: the current election cycle number.
	term int

	// Internal Only: whether or not this item represents configuration
	config bool
}

func newEventLogItem(i int, t int, e Event) LogItem {
	return LogItem{Index: i, term: t, Event: e}
}

func newConfigLogItem(i int, t int, c bool) LogItem {
	return LogItem{Index: i, term: t, config: c}
}

func readLogItem(r scribe.Reader) (interface{}, error) {
	var err error
	var item LogItem
	var bytes []byte
	err = common.Or(err, r.ReadInt("index", &item.Index))
	err = common.Or(err, r.ReadInt("term", &item.term))
	err = common.Or(err, r.ReadBytes("event", &bytes))
	if err != nil {
		return item, err
	}
	item.Event = Event(bytes)
	return item, err
}

func parseItem(bytes []byte) (LogItem, error) {
	msg, err := scribe.Parse(bytes)
	if err != nil {
		return LogItem{}, err
	}

	raw, err := readLogItem(msg)
	if err != nil {
		return LogItem{}, err
	}

	return raw.(LogItem), nil
}

func (l LogItem) Write(w scribe.Writer) {
	w.WriteInt("index", l.Index)
	w.WriteInt("term", l.term)
	w.WriteBytes("event", l.Event.Raw())
}

func (l LogItem) Bytes() []byte {
	return scribe.Write(l).Bytes()
}

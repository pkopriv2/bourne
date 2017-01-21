package kayak

import (
	"errors"
	"fmt"
	"io"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
)

var (
	ClosedError    = errors.New("Kayak:Closed")
	NotLeaderError = errors.New("Kayak:NotLeader")
	NoLeaderError  = errors.New("Kayak:NoLeader")
)

var (
	AccessError      = errors.New("Kayak:AccessError")
	EndOfStreamError = errors.New("Kayak:EndOfStream")
	OutOfBoundsError = errors.New("Kayak:OutOfBounds")
	InvariantError   = errors.New("Kayak:InvariantError")
)

// Runs the machine, using a replicated event log as the durable storage
// engine.  Machines of this type can continue to function as long as a
// majority of peers remain alive.  However, in the event that only a
// minority of peers remain, they will continue to hold elections
// indefinitely.
//
func Run(ctx common.Context, app Machine, self string, peers []string) error {
	return nil
}

// Events are the fundamental unit of replication.  This the primary
// consumer data structure used in interacting with the replicated log.
// The onus of interpreting an event is on the consumer.
type Event []byte

func (e Event) Raw() []byte {
	return []byte(e)
}

func (e Event) Write(w scribe.Writer) {
	w.WriteBytes("raw", e.Raw())
}

type Client interface {
	Run(log Log, snapshot <-chan Event) error
}

// A machine is anything that is expressable as a sequence of events.
//
// Kayak allows consumers to utilize a generic, replicated event
// log in order to create highly-resilient, strongly consistent
// replicated state machines.
//
// # Distributed Consensus
//
// Underpinning every machine is a log that implements the Raft
// distributed consensus protocol.  While most of the details of the
// protocol are abstracted, it is useful to know some of the high level
// details.
//
// At startup, the machines are aware of each other's existence
// and form a fully-connected graph between them.  The machines
// use the Raft leader election protocol to establish a leader
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
// This one property can make the management of a distributed log a very
// tractable problem, even when exposed directly to consumer machines.
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
//                               |-------*commit*------|      |
//                               v                     |      |
// {Consumer}---*updates*--->{Machine}---*append*--->{Log}<---+--->{Peer}
//                               |					   ^      |
//                               |-------*compact*-----|      |
//                                                            |
//
//
// This brings us to the main issue of machine design:
//
// * Appends and commits are separate, unsynchronized streams.
//
// Moreover, users of these APIs must not make any assumptions about the
// relationship of one stream to the other.  In other words, a successful
// append ONLY gives the guarantee that it has been committed to a majority
// of peers, and not necessarily to itself.  If the consuming machine requires
// strong consistency, synchronizing the request with the commit stream
// is likely required.
//
// # Log Compactions
//
// For many machines, their state is actually represented by many redundant
// log items.  In other words, many of the items have been obviated.  The
// machine can provide the log of a shortened 'snapshot' version of itself
// using the Log#Compact() method.
//
type Machine interface {

	// Run invokes the machines main loop.
	//
	// Each time the machine starts, it is provided a snapshot of events
	// that can be used to rebuild machine state.
	//
	// Consumers may choose to return from this method at any time.  If
	// the returned error is nil, the log shuts down and returns from
	// the main kayak.Run() method.  If the returned value is not nil,
	// the machine is automatically restarted with the latest snapshot.
	//
	// Consumers wishing to terminate the log permanently should close
	// the log directly and return from the main loop.  Any errors
	// returned at that time are returned from the main kayak.Run()
	// loop.
	//
	Run(log Log, snapshot <-chan Event) error
}

// The log is a durable, replicated event sequence
type Log interface {
	io.Closer

	// Head returns the index of the latest item in the log.
	Head() int

	// Listen generates a listener interested in log commits starting from
	// and including the start index.
	//
	// The listener is guaranteed to receive ALL items in the order
	// they are committed - however - if a listener becomes significantly
	// lagged, so much so that its current segment is compacted away,
	// it will fail.  However, it is not expected that consumers need be
	// concerned with log state.  In many cases, simply returning the
	// error from the main machine's run loop and restarting is sufficient
	// to resume operations.
	//
	// This method also gives control over the underlying buffer size.
	// Consumers which need highly synchronized state with the log should
	// choose smaller buffers.
	//
	Listen(start int, buf int) (Listener, error)

	// Append appends and commits the event to the log.
	//
	// If the append is successful, Do not assume that all committed items
	// have been replicated to this instance.  Appends should always accompany
	// a sync routine that ensures that the log has been caught up prior to
	// returning control.
	//
	Append(Event) (LogItem, error)

	// Compact replaces the log until the given point with the given snapshot
	//
	// In order to guard against premature termination of a snapshot, the
	// consumer must provide the log with an expectation of the snapshot size.
	// Only once all items in the snapshot have been stored can the snapshot
	// take effect.  If the snapshot stream is terminated prematurely,
	// the compaction will abort.
	//
	// Once the snapshot has been safely stored, the log until and including
	// the index will be deleted. This method is synchronous, but can be called
	// concurrent to other log methods. It should be considered safe for the
	// machine to continue to serve requests normally while a compaction is
	// processing.
	//
	// Concurrent compactions are possible, however, the log ensures that
	// the only the latest snapshot is retained. In the event that a compaction
	// is obsoleted concurrently, no error will be returned.
	//
	Compact(until int, snapshot <-chan Event, size int) error
}

// A listener represents a stream of log items.
type Listener interface {
	io.Closer

	// Returns the next log item in the listener.  If an error is returned,
	// it may be for a variety of reasons:
	//
	// * The underlying log has been closed.
	// * The underlying log has been compacted away.  (*Important: See Below)
	// * There was a system/disc error.
	//
	// If a reader gets significantly behind the underlying log's end, it
	// is possible for the listener to become corrupted if the underlying log
	// is compacted away. Consumers are allowed to choose how to deal with this,
	// but for in-memory state machines, if the stream was in critical path
	// to machine state, then it's probably best to just rebuild the machine.
	//
	Next() (LogItem, error)
}

// The basic log item.
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

func (l LogItem) String() string {
	return fmt.Sprintf("Item(idx=%v,term=%v,size=%v)", l.Index, l.term, len(l.Event))
}

func (l LogItem) Write(w scribe.Writer) {
	w.WriteInt("index", l.Index)
	w.WriteInt("term", l.term)
	w.WriteBytes("event", l.Event)
}

func (l LogItem) Bytes() []byte {
	return scribe.Write(l).Bytes()
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

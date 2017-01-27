package kayak

import (
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

var (
	ClosedError    = errors.New("Kayak:Closed")
	NotLeaderError = errors.New("Kayak:NotLeader")
	NotMemberError = errors.New("Kayak:NotMember")
	// NoLeaderError  = errors.New("Kayak:NoLeader")
	// NotPeerError   = errors.New("Kayak:NotPeer")
)

func Start(ctx common.Context, app Machine, self string) error {
	return nil
}

func Join(ctx common.Context, app Machine, self string, peer string) error {
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
	w.WriteBytes("bytes", e)
}

func eventParser(r scribe.Reader) (interface{}, error) {
	var event Event
	if e := r.ReadBytes("bytes", (*[]byte)(&event)); e != nil {
		return nil, e
	}
	return event, nil
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
//                  *Local Process*                           |  *Network Process*
//                                                            |
//                               |-------*commit*------|      |
//                               v                     |      |
// {Consumer}---*updates*--->{Machine}---*append*--->{Log}<---+--->{Peer}
//                               |                     ^      |
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
// using the Log#Compact() method.  On startup, the latest snapshot
// is delivered to the machine in order to give the machine an opportunity
// to rebuild its state.
//
// # Linearizability
//
// The raft
//
//
type Machine interface {

	// Run invokes the machines main loop.
	//
	// Consumers may choose to return from this method at any time.  If
	// the returned error is nil, the log shuts down and returns from
	// the main kayak.Run() method.  If the returned value is not nil,
	// the machine is automatically restarted.
	//
	// // Consumers wishing to terminate the log permanently should close
	// // the log directly and return from the main loop.  Any errors
	// // returned at that time are returned from the main kayak.Run()
	// // loop.
	//
	// FIXME: Better semantics around exiting!
	Run(log Log, sync Sync) error
}

// The log is a durable, replicated event sequence
type Log interface {
	io.Closer

	// Returns the latest snaphot from the log.  Useful to rebuild
	// internal machine state.
	Snapshot() (until int, events <-chan Event, err error)

	// Listen generates a listener interested in log commits starting from
	// and including the start index.
	//
	// This listener guarantees that items are delivered with exactly-once
	// semantics.  Because log replication can only provide at-least-once
	// semantics, every listener will filter duplicate requests.
	//
	// This method also gives control over the underlying buffer size.
	// Consumers which need highly synchronized state with the log should
	// choose smaller buffers.
	Listen(start int, buf int) (Listener, error)

	// Append appends and commits the event to the log.
	//
	// If the append is successful, Do not assume that all committed items
	// have been replicated to this instance.  Appends should always accompany
	// a sync routine that ensures that the log has been caught up prior to
	// returning control.
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
	// Concurrent compactions are possible, however, an invariant of the log
	// is that it must always progress.  Therefore, an older snapshot cannot
	// usurp a newer one.
	Compact(until int, snapshot <-chan Event, size int) error
}

// The synchronizer gives the consuming machine the ability to synchronize
// its state with other members of the cluster.  This is critical for
// machines to be able to implement linearizable semantics.  This also gives
// those with less strict requirements the ability to issue stale requests.
//
// In order to give linearizable reads, consumers can query for a "read-barrier"
// index. This index is the maximum index that has been applied to all machines
// within the cluster.  With this barrier, machines can ensure their
// internal state has been caught up to the time the read was initiated,
// thereby obtaining linearizability for the operation.
//
type Sync interface {

	// Ack tells the synchronizer that the index (and everything that preceded)
	// it has been applied to the state machine. This operation is commutative,
	// meaning that if a lower index is applied after a later index, then
	// only the latest is used for synchronization purposes.
	Ack(index int)

	// Returns the current read-barrier for the cluster.
	Barrier() (int, error)

	// Sync waits for the machine to be caught up to the barrier.
	Sync(timeout time.Duration, barrier int)
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
	// This method is not safe for concurrent access.
	Next() (LogItem, error)
}

type LogItem struct {

	// Item index. May be used to reconcile state with log.
	Index int

	// The event bytes.  This is the fundamental unit of replication.
	Event Event

	// Internal Only: the current election cycle number.
	Term int

	// Internal Only: (used to filter duplicate appends)
	Source uuid.UUID

	// Internal Only: (used to filter duplicate appends)
	Seq int

	// Internal Only: Used for system level events (e.g. config, noop, etc...)
	Kind int
}

func NewLogItem(i int, e Event, t int, s uuid.UUID, seq int, k int) LogItem {
	return LogItem{i, e, t, s, seq, k}
}

var (
	Std    = 0
	NoOp   = 1
	Config = 2
)

func (l LogItem) String() string {
	return fmt.Sprintf("Item(idx=%v,term=%v,size=%v)", l.Index, l.Term, len(l.Event))
}

func (l LogItem) Write(w scribe.Writer) {
	w.WriteInt("index", l.Index)
	w.WriteBytes("event", l.Event)
	w.WriteInt("term", l.Term)
	w.WriteUUID("source", l.Source)
	w.WriteInt("seq", l.Seq)
	w.WriteInt("kind", l.Kind)
}

func (l LogItem) Bytes() []byte {
	return scribe.Write(l).Bytes()
}

func ReadLogItem(r scribe.Reader) (interface{}, error) {
	var err error
	var item LogItem
	err = common.Or(err, r.ReadInt("index", &item.Index))
	err = common.Or(err, r.ReadInt("term", &item.Term))
	err = common.Or(err, r.ReadBytes("event", (*[]byte)(&item.Event)))
	err = common.Or(err, r.ReadUUID("source", &item.Source))
	err = common.Or(err, r.ReadInt("seq", &item.Seq))
	err = common.Or(err, r.ReadInt("kind", &item.Kind))
	return item, err
}

func ParseItem(bytes []byte) (LogItem, error) {
	msg, err := scribe.Parse(bytes)
	if err != nil {
		return LogItem{}, err
	}

	raw, err := ReadLogItem(msg)
	if err != nil {
		return LogItem{}, err
	}

	return raw.(LogItem), nil
}

// Storage apis,errors
var (
	InvariantError   = errors.New("Kayak:Invariant")
	EndOfStreamError = errors.New("Kayak:EndOfStream")
	OutOfBoundsError = errors.New("Kayak:OutOfBounds")
	CompactionError  = errors.New("Kayak:Compaction")
)

type LogStore interface {
	Get(id uuid.UUID) (StoredLog, error)
	New(uuid.UUID, []byte) (StoredLog, error)
}

type StoredLog interface {
	Id() uuid.UUID
	Last() (int, int, error)
	Truncate(start int) error
	Scan(beg int, end int) ([]LogItem, error)
	Append(Event, int, uuid.UUID, int, int) (LogItem, error)
	Get(index int) (LogItem, bool, error)
	Insert([]LogItem) error
	Compact(until int, ch <-chan Event, size int, config []byte) (StoredSnapshot, error)
	Snapshot() (StoredSnapshot, error)
}

type StoredSnapshot interface {
	LastIndex() int
	LastTerm() int
	Size() int
	Config() []byte
	Scan(beg int, end int) ([]Event, error)
	Delete() error
}

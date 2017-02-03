// FIXME: Fix docs now that api has changed.
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
package kayak

import (
	"errors"
	"fmt"
	"io"

	"github.com/boltdb/bolt"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// Core api errors
var (
	ClosedError    = errors.New("Kayak:ClosedError")
	NotLeaderError = errors.New("Kayak:NotLeaderError")
	NotSlaveError  = errors.New("Kayak:NotMemberError")
	NoLeaderError  = errors.New("Kayak:NoLeaderError")
	TimeoutError   = errors.New("Kayak:TimeoutError")
	ExpiredError   = errors.New("Kayak:ExpiredError")
	CanceledError  = errors.New("Kayak:CanceledError")
)

// Storage api errors
var (
	InvariantError   = errors.New("Kayak:InvariantError")
	EndOfStreamError = errors.New("Kayak:EndOfStreamError")
	OutOfBoundsError = errors.New("Kayak:OutOfBoundsError")
	CompactionError  = errors.New("Kayak:CompactionError")
)

type Dependencies struct {
	Ctx      common.Context
	LogStore LogStore
	Network  net.Network
	Db       *bolt.DB
}

func DefaultDependencies(ctx common.Context) Dependencies {
	return Dependencies{}
}

// Extracts out the raw errors.
func Cause(err error) error {
	return extractError(err)
}

func Start(ctx common.Context, deps Dependencies, addr string) (Peer, error) {
	host, err := newHost(ctx, deps.Network, deps.LogStore, deps.Db, addr)
	if err != nil {
		return nil, err
	}

	return host, host.Start()
}

func Join(ctx common.Context, deps Dependencies, addr string, peers []string) (Peer, error) {
	host, err := newHost(ctx, deps.Network, deps.LogStore, deps.Db, addr)
	if err != nil {
		return nil, err
	}

	return host, host.Join(peers[0])
}

// A peer is a member of a cluster that is actively participating in log replication.
type Peer interface {
	io.Closer

	// The unique identifier for this peer.
	Id() uuid.UUID

	// Hostname of self.
	Hostname() string

	// Hostnames of all members in the cluster (*dynamic*)
	Roster() []string

	// Returns the cluster synchronizer.
	Sync() (Sync, error)

	// Retrieves a connection to the log.
	Log() (Log, error)
}

type Log interface {

	// Returns the latest snaphot from the log.
	Snapshot() (int, EventStream, error)

	// Listen generates a stream of committed log entries starting at and
	// including the start index.
	//
	// This listener guarantees that entries are delivered with at-least-once
	// semantics.
	Listen(start int, buf int) (Listener, error)

	// Append appends and commits the event to the log.
	//
	// If the append is successful, Do not assume that all committed items
	// have been replicated to this instance.  Appends should always accompany
	// a sync routine that ensures that the log has been caught up prior to
	// returning control.
	Append(cancel <-chan struct{}, event Event) (Entry, error)

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
// machines to be able to prevent stale reads.
//
// In order to give linearizable reads, consumers can query for a "read-barrier"
// index. This index is the maximum index that has been applied to any machine
// within the cluster.  With this barrier, machines can ensure their
// internal state has been caught up to the time the read was initiated,
// thereby obtaining linearizability for the operation.
type Sync interface {

	// Applied tells the synchronizer that the index (and everything that preceded)
	// it has been applied to the state machine. This operation is commutative,
	// meaning that if a lower index is applied after a later index, then
	// only the latest is used for synchronization purposes.
	Ack(index int)

	// Returns the current read-barrier for the cluster.
	Barrier(cancel <-chan struct{}) (int, error)

	// Sync waits for the local machine to be caught up to the barrier.
	Sync(cancel <-chan struct{}, index int) error
}

// A listener is just a stream of entries.
type Listener interface {
	io.Closer
	Ctrl() common.Control
	Data() <-chan Entry
}

// An event is just a stream of events.
type EventStream interface {
	io.Closer
	Ctrl() common.Control
	Data() <-chan Event
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

// An Entry represents an entry in the replicated log.
type Entry struct {
	Index int
	Event Event
	Term  int
	Kind  Kind
}

var (
	Std    Kind = 0
	NoOp   Kind = 1
	Config Kind = 2
)

type Kind int

func (k Kind) String() string {
	switch k {
	default:
		return "Unknown"
	case Std:
		return "Std"
	case NoOp:
		return "NoOp"
	case Config:
		return "Config"
	}
}

func (l Entry) String() string {
	return fmt.Sprintf("Item(idx=%v,term=%v,kind=%v,size=%v)", l.Index, l.Term, l.Kind, len(l.Event))
}

func (l Entry) Write(w scribe.Writer) {
	w.WriteInt("index", l.Index)
	w.WriteBytes("event", l.Event)
	w.WriteInt("term", l.Term)
	w.WriteInt("kind", int(l.Kind))
}

func (l Entry) Bytes() []byte {
	return scribe.Write(l).Bytes()
}

func readEntry(r scribe.Reader) (entry Entry, err error) {
	err = common.Or(err, r.ReadInt("index", &entry.Index))
	err = common.Or(err, r.ReadInt("term", &entry.Term))
	err = common.Or(err, r.ReadBytes("event", (*[]byte)(&entry.Event)))
	err = common.Or(err, r.ReadInt("kind", (*int)(&entry.Kind)))
	return
}

func parseEntry(r scribe.Reader) (interface{}, error) {
	return readEntry(r)
}

func parseEntryBytes(bytes []byte) (Entry, error) {
	msg, err := scribe.Parse(bytes)
	if err != nil {
		return Entry{}, err
	}

	return readEntry(msg)
}

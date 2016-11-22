package convoy

import (
	"io"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

const (
	confPingTimeout         = "convoy.ping.timeout"
	confUpdateTimeout       = "convoy.update.timeout"
	confUpdateBatchSize     = "convoy.update.batch.size"
	confDisseminationPeriod = "convoy.dissemination.period"
	confServerPoolSize      = "convoy.server.pool.size"
)

const (
	defaultPingTimeout         = time.Second
	defaultUpdateTimeout       = time.Second
	defaultUpdateBatchSize     = 50
	defaultDisseminationPeriod = 5 * time.Second
	defaultServerPoolSize      = 10
)

// References:
//  * Mathematical Analysis: http://se.inf.ethz.ch/old/people/eugster/papers/gossips.pdf
//  * Mathematical Analysis: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.557.1902&rep=rep1&type=pdf
//  * https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf
//  * https://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf
//  * Basic Design: http://bitsavers.informatik.uni-stuttgart.de/pdf/xerox/parc/techReports/CSL-89-1_Epidemic_Algorithms_for_Replicated_Database_Maintenance.pdf
//
// Convoy is a very specialized replicated database and dissemination library.
//
// The intended use is primarily within infrastructure management, where the
// desire is to understand not only what what hosts are currently alive and
// healthy, but also meta information about them.  In that vein, hosts are
// able to publish information about themselves.  This can range from standard
// properties (e.g. hostname, fqdn, ip address, etc..) but can really be anything.
//
// Architecturally, this means that each member of a cluster just manages its
// own database - ie all updates to that database are routed to it - and
// shares updates with other members.
//
// The end goal is a highly resilient, searchable dataset that allows members
// to be looked up via their store properties.
//

// Publishes the db to the given port.  This is the "first" member of the
// cluster and will not discover anyone else until it is contacted.
func Publish(ctx common.Context, db Database) (Cluster, error) {
	return nil, nil
}

// Publishes the store to the cluster via the given member addr.
func PublishTo(ctx common.Context, db Database, addr string) (Cluster, error) {
	return nil, nil
}

// A database is really just an indexed log of changes.  In fact, a store
// is just the aggregated list of changes.
type Database interface {
	Store

	// the only difference between a store and a db is that a database
	// has a durable change log
	Log() ChangeLog
}

// A change log is nothing but an ordered list of changes.
type ChangeLog interface {
	io.Closer

	// Every change log must be globally unique
	Id() (uuid.UUID, error)

	// Returns the current sequence of the log
	Seq() (int, error)

	// Appends a change to the log and notifies any listeners.
	Append(key string, val string, del bool) (Change, error)

	// Returns all the changes in the lifetime of the change log.
	All() ([]Change, error)

	// Registers a handler to be invoked on any change to the log.
	// The zero value and false are sent when the log is closed.
	Listen(func(Change, bool))
}

// Fundamental unit of change within the published database
type Change struct {
	Seq int
	Key string
	Val string
	Ver int
	Del bool
}

func ReadChange(r scribe.Reader) (Change, error) {
	c := &Change{}
	if err := r.Read("seq", &c.Seq); err != nil {
		return *c, err
	}
	if err := r.Read("key", &c.Key); err != nil {
		return *c, err
	}
	if err := r.Read("val", &c.Val); err != nil {
		return *c, err
	}
	if err := r.Read("ver", &c.Ver); err != nil {
		return *c, err
	}
	if err := r.Read("del", &c.Del); err != nil {
		return *c, err
	}
	return *c, nil
}

func (c Change) Write(w scribe.Writer) {
	w.Write("seq", c.Seq)
	w.Write("key", c.Key)
	w.Write("val", c.Val)
	w.Write("ver", c.Ver)
	w.Write("del", c.Del)
}

// A cluster represents the aggregated view of all members' data
// stores.
type Cluster interface {
	io.Closer

	// Returns a handle to the store of the given id.  Consumers
	GetMember(id uuid.UUID) (Member, error)
	//
	// // Returns the currently alive members.
	// Alive() ([]Member, error)
	//
	// // Returns the currently dead members.  This means the member is out of contact.
	// // This can happen for a variety of environmental reasons.
	// Dead() ([]Member, error)
	//
	// // Returns the recently left members.
	// Gone() ([]Member, error)
	//
	// // Return
	// ForceLeave(m Member)

	// MUST DO:
	// Add search functiionality.
}

// A simple client abstraction.
type Member interface {

	// // The id of the member
	// Ping() bool
	//
	// // Connects to the target of the client on the given port.
	// Connect(int) (net.Connection, error)
	//
	// // Returns a store client.
	// Store(common.Context) (Store, error)
}

// A very simple key,value store abstraction. Updates to the store
// are transactionally safe - regardless of whether the store is
// local or remote.
type Store interface {
	io.Closer

	// Returns the item value and version
	Get(key string) (string, bool, error)

	// Returns a handle to a batch of changes to apply to the store.
	Put(key string, val string) error

	// Deletes the value associated with the key
	Del(key string) error
}

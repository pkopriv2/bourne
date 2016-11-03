package convoy

import (
	"io"

	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

// References:
//  * Mathematical Analysis: http://se.inf.ethz.ch/old/people/eugster/papers/gossips.pdf
//  * Mathematical Analysis: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.557.1902&rep=rep1&type=pdf
//  * https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf
//  * https://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf
//  * Basic Design: http://bitsavers.informatik.uni-stuttgart.de/pdf/xerox/parc/techReports/CSL-89-1_Epidemic_Algorithms_for_Replicated_Database_Maintenance.pdf
//
// Convoy is a database dissemination library.  The intended use is primarily
// within infrastructure management, where the desire is to understand
// not only what what hosts are currently alive and healthy, but also
// meta information about that host.  In that vein, hosts are able to
// publish information about themselves. Moreover, clients of the cluster
// can coordinate with a host to update the host's database.
//
// Convoy replicates each store to all members of the cluster using
// an epidemic style dissemination protocol.  The goal is a searchable
// data store which identifies hosts - or its properties.
//

// Publishes the db to the given port.  This is the "first" member of the
// cluster.
func Publish(ctx common.Context, db Database, port int) (Cluster, error) {
	return nil, nil
}

// Publishes the store to the cluster via the given member addr.  The
func PublishTo(ctx common.Context, db Database, addr string) (Cluster, error) {
	return nil, nil
}

// A cluster represents the aggregated view of all members' data stores.
type Cluster interface {
	io.Closer

	// Returns a handle to the store of the given id.  Consumers
	// must close the store or risk leaking resources.
	GetStore(store uuid.UUID) (Store, error)

	// Returns the value of the given key from the given store.
	GetValue(store uuid.UUID, key string) (string, error)

	// MUST DO:
	// Add search functiionality.
}

// A database is really just an indexed log of changes.
type Database interface {
	Store

	// Returns a channel containing an ordered list of changes
	// whose versions are greater than or equal to the given
	// version number.
	Log(int) <-chan Change
}

// A change is the funamental dissemination unit
type ChangeType int

const (
	Publish    ChangeType = 0
	Leave                 = 1
	Offline               = 2
	ItemUpdate            = 3
	ItemDelete            = 4
)

//
type Transaction struct {

	// Which store is being updated
	Re uuid.UUID

	// At what version was the store.
	Version int

	// The type of change being applied
	Type ChangeType

	// The data being updated (if applicable)
	Key string
	Val string
}

// A very simple key,value store abstraction. Updates to the store
// are transactionally safe - regardless of whether the store is
// local or remote.
type Store interface {
	io.Closer

	// A value
	Id() uuid.UUID

	// Returns the item value and version
	Get(key string) (string, error)

	// Returns a handle to a batch of changes to apply to the store.
	Update() (Update, error)
}

// An update represents an uncommitted batch of changes for a given store.
type Update interface {

	// Puts the key value
	Put(key string, val string)

	// Deletes the key value at key.
	Delete(key string)

	// Commits the changes and returns the new version.
	Commit() (int, error)
}

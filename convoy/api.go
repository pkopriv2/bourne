package convoy

import (
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// Public Error Types
var (
	EvictedError = errors.New("Convoy:Evicted")
	FailedError  = errors.New("Convoy:Failed")
	ClosedError  = errors.New("Convoy:Closed")
)

// Public Configuration
var Config = struct {

	// The number of attempts to try to rejoin a cluster on failure.
	JoinAttempts string

	// The number of probes to send out to determine if a client is
	// truly unreachable.  In the event of that a member is no longer
	// reachable, they are deemed unhealthy and evicted from the cluster.
	HealthProbeCount string

	// The timeout used during health checks
	// See README.md  for more information on member health.
	HealthProbeTimeout string

	// The dissemination represents the number of ln(N) times a message
	// is redistributed.
	DisseminationFactor string

	// The dissemination period.
	DisseminationPeriod string

	// The disssemination batch size.  This along with the dissemination
	// period allows us to cap the overall network uses at each node
	// in events per period.
	DisseminationBatchSize string
}{
	"convoy.join.attempts.count",
	"convoy.health.probe.count",
	"convoy.health.probe.timeout",
	"convoy.dissemination.factor",
	"convoy.dissemination.period",
	"convoy.dissemination.size",
}

const (
	defaultJoinAttemptsCount   = 10
	defaultHealthProbeTimeout  = 10 * time.Second
	defaultHealthProbeCount    = 3
	defaultDisseminationFactor = 3
	defaultDisseminationPeriod = 500 * time.Millisecond
	defaultServerPoolSize      = 20
)

// Publishes the db to the given port.  This is the "first" member of the
// cluster and will not discover anyone else until it is contacted.
func StartSeedHost(ctx common.Context, path string, addr string) (host Host, err error) {
	var db *database

	db, err = openDatabase(ctx, path)
	if err != nil {
		return nil, errors.Wrap(err, "Error opening db [%v]")
	}
	defer common.RunIf(func() { db.Close() })(err)

	// host, err = newSeedHost(ctx, db, addr)
	return nil, nil
}

func StartHost(ctx common.Context, path string, addr string, peer string) (host Host, err error) {
	db, err := openDatabase(ctx, path)
	if err != nil {
		return nil, errors.Wrapf(err, "Error opening db [%v]", path)
	}
	defer common.RunIf(func() { db.Close() })(err)
	// host, err = newHost(ctx, path, addr, peer)
	return nil, nil
}

// A host is the local member participating in and disseminating a shared
// directory.
type Host interface {
	io.Closer

	// A host is a local member.
	Member

	// Performs an immediate shutdown of the host
	Shutdown() error

	// Provides access to the distributed directory.
	Directory() Directory
}

// A member is just that - a member of a cluster.
type Member interface {

	// The id of the member
	Id() uuid.UUID

	// Every member is versioned in accordance with their
	// membership.  The implication is that members of
	// the same id, but distint versions are unique.
	Version() int

	// Connects to the member on the provided port.  Consumers
	// are responsible for closing the connection.
	Connect(int) (net.Connection, error)

	// Returns a store client.  Consumers are responsible for closing
	// the store once they are finished.
	Store(common.Context) (Store, error)
}

// The directory is the central storage unit that hosts all information
// on all other members of the cluster.  All methods on the directory
// can fail, if the parent has closed or failed for any reason.
type Directory interface {

	// Retrieves the replica with the given id.  Nil if the member
	// doesn't exist.
	Get(id uuid.UUID) (Member, error)

	// Returns all of the currently active members.
	All() ([]Member, error)

	// Evicts a member from the cluster.  The member will NOT automatically
	// rejoin on eviction.
	Evict(Member) error

	// Marks a member as being failed.  The next time the member contacts an
	// infected member, he will be forced to leave the cluster and rejoin.
	Fail(Member) error

	// Runs the input filter function over all active members's stores -
	// returning those for which the function returns true.  Searches
	// block concurrent updates to the store.
	Search(filter func(uuid.UUID, string, string) bool) ([]Member, error)

	// Runs the input filter function over all active members's stores -
	// returning the first member for which the function returns true
	First(filter func(uuid.UUID, string, string) bool) (Member, error)
}

// A very simple key,value store abstraction. This store uses
// optimistic locking to provide a single thread-safe api for
// both local and remote stores.
//
// If this is the local store, closing the store will NOT disconnect
// the replica, it simply prevents any changes to the store from
// occurring.
type Store interface {
	io.Closer

	// Returns true and the item or false an the zero value.
	//
	// If the return value inclues an error, the other results should
	// not be trusted.
	Get(key string) (found bool, item Item, err error)

	// Updates the value at the given key if the version matches.
	// Returns a flag indicating whether or not the operation was
	// successful (ie the version matched) and if so, the updated
	// value.  Otherwise an error is returned.
	//
	// If the return value inclues an error, the other results should
	// not be trusted.
	Put(key string, val string, expected int) (bool, Item, error)

	// Deletes the value at the given key if the version matches.
	// Returns a flag indicating whether or not the operation was
	// successful (ie the version matched) and if so, the updated
	// value.  Otherwise an error is returned.
	//
	// If the return value inclues an error, the other results should
	// not be trusted.
	Del(key string, expected int) (bool, Item, error)
}

// An item in a store.
type Item struct {
	Val string
	Ver int
	Del bool
}

package convoy

import (
	"io"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

const (

	// The timeout used during health checks and event propagation.
	// See README.md  for more information on member health.
	ConnectTimeout = "convoy.connect.timeout"

	// The number of attempts to try to rejoin a cluster on failure.
	JoinRetryAttempts = "convoy.join.retry.attempts"

	// The number of probes to send out to determine if a client is
	// truly unreachable.  In the event of that a member is no longer
	// reachable, they are deemed unhealthy and evicted from the cluster.
	HealthProbeCount = "convoy.health.probe.size"

	// The dissemination represents the number of ln(N) times a message
	// is redistributed.
	DisseminationFactor = "convoy.dissemination.factor"

	// The dissemination period.
	DisseminationPeriod = "convoy.dissemination.period"

	// The disssemination batch size.  This along with the dissemination
	// period allows us to cap the overall network uses at each node
	// in events per period.
	DisseminationBatchSize = "convoy.dissemination.size"
)

const (
	defaultConnectTimeout      = 10 * time.Second
	defaultHealthProbeCount    = 3
	defaultDisseminationFactor = 3
	defaultDisseminationPeriod = 500 * time.Millisecond
	defaultServerPoolSize      = 20
)

// Publishes the db to the given port.  This is the "first" member of the
// cluster and will not discover anyone else until it is contacted.
func StartSeedHost(ctx common.Context, addr string) (Host, error) {
	return nil, nil
}

func StartHost(ctx common.Context, addr string, peer string) (Host, error) {
	return nil, nil
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

	// Returns the item value and version
	Get(key string) (*Item, error)

	// Returns a handle to a batch of changes to apply to the store.
	Put(key string, val string, expected int) (bool, Item, error)

	// Deletes the value associated with the key
	Del(key string, expected int) (bool, Item, error)
}

// An item in a store.
type Item struct {
	Val string
	Ver int
	Del bool
}

// A member is just that - a member of a cluster.
type Member interface {

	// The id of the member
	Id() uuid.UUID

	// The current version of the member's membership.
	Version() int

	// Connects to the member on the provided port.  Consumers
	// are responsible for closing the connection.
	Connect(int) (net.Connection, error)

	// Returns a store client.  Consumers are responsible for closing
	// the store once they are finished.
	Store(common.Context) (Store, error)
}

// The directory is the central storage unit that hostsl information
// on all other members of the cluster.
type Directory interface {

	// Retrieves the replica with the given id.  Nil if the member
	// doesn't exist.
	Get(id uuid.UUID) (Member, error)

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

// A host is a member participating in and disseminating a shared directory.
type Host interface {
	io.Closer
	Member

	// Provides access to the distributed directory.
	Directory() (Directory, error)
}

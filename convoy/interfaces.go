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
func StartSeedReplica(ctx common.Context, port int) (Replica, error) {
	return nil, nil
}

func StartReplica(ctx common.Context, port int, addr string) (Replica, error) {
	return nil, nil
}

// A very simple key,value store abstraction. Updates to the store
// are transactionally safe - regardless of whether the store is
// local or remote.  The store uses optimistic versioning to
// maintain thread safety - even for network stores.
//
// If this is the local store, closing the store will NOT disconnect
// the replica, it simply prevents any changes to the store from
// occurring.
type Store interface {
	io.Closer

	// Returns the item value and version
	Get(key []byte) (val []byte, ver int, err error)

	// Returns a handle to a batch of changes to apply to the store.
	Put(key []byte, val []byte, ver int) error

	// Deletes the value associated with the key
	Del(key []byte, ver int) error
}

// A member is just that - a member of a cluster.
type Member interface {

	// The published hostname.  It must be resolvable by the underlying
	// network resources.
	Hostname() string

	// Connects to the member on the provided port.  Consumers
	// are responsible for closing the connection.
	Connect(int) (net.Connection, error)

	// Returns a store client.  Consumers are responsible for closing
	// the store once they are finished.
	Store() (Store, error)
}

// A replica is a connected member of a cluster.
type Replica interface {
	Member

	// Retrieves the replica with the given id.  Nil if the member
	// doesn't exist.
	GetMember(id uuid.UUID) (Member, error)

	// Runs the input filter function over all active members's stores -
	// returning those for which the function returns true.  Searches
	// block concurrent updates to the store.
	Search(filter func(uuid.UUID, []byte, []byte) bool) ([]Member, error)
}

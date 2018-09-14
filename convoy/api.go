package convoy

import (
	"fmt"
	"io"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

// Public Error Types
var (
	EvictedError = errors.New("Convoy:Evicted")
	FailedError  = errors.New("Convoy:Failed")
	JoinError    = errors.New("Convoy:JoinError")
	ClosedError  = errors.New("Convoy:Closed")
)

type Options struct {
	Network net.Network
	Storage *bolt.DB
}

// Publishes the db to the given port.  This is the "first" member of the
// cluster and will not discover anyone else until it is contacted.
func Start(ctx common.Context, addr string, fns ...func(*Options)) (Host, error) {
	opts, err := buildOptions(ctx, fns)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	db, err := openDatabase(ctx, openChangeLog(ctx, opts.Storage))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	host, err := newHost(ctx, db, opts.Network, addr, nil)
	if err == nil {
		return host, nil
	} else {
		return nil, err
	}
}

func Join(ctx common.Context, addr string, peers []string, fns ...func(*Options)) (Host, error) {
	opts, err := buildOptions(ctx, fns)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	db, err := openDatabase(ctx, openChangeLog(ctx, opts.Storage))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	host, err := newHost(ctx, db, opts.Network, addr, peers)
	if err == nil {
		return host, nil
	} else {
		return nil, err
	}
}

// A host is the local member participating in and disseminating a shared
// directory.
type Host interface {
	io.Closer

	// The id of the host
	Id() uuid.UUID

	// The local member
	Self() (Member, error)

	// The local directory
	Directory() (Directory, error)

	// Performs an immediate shutdown of the host.
	//
	// !! Not safe for production use !! Always try to leave a cluster gracefully.
	//
	Shutdown() error
}

// A member is just that - a member of a cluster.
type Member interface {

	// The id of the member
	Id() uuid.UUID

	// The network address of themember
	Addr() string

	// Every member is versioned in accordance with their membership.  The implication is
	// that members of the same id, but distint versions are unique.
	Version() int

	// Connects to the member on the provided port.  Consumers
	// are responsible for closing the connection.
	Connect(net.Network, time.Duration, int) (net.Connection, error)
}

// The directory is the central storage unit that hosts all information
// on all other members of the cluster.  All methods on the directory
// can fail, if the parent has closed or failed for any reason.
type Directory interface {
	io.Closer

	// Starts listening for joins & evictions. (currently only availabe for local directory)
	ListenRoster() (RosterListener, error)

	// Starts listening for failures & healthy. (currently only availabe for local directory)
	ListenHealth() (HealthListener, error)

	// Evicts a member from the cluster.  The member will NOT automatically rejoin on eviction.
	EvictMember(cancel <-chan struct{}, m Member) error

	// Marks a member as being failed.  This is the same as an eviction except the member will
	// rejoin automatically
	FailMember(cancel <-chan struct{}, m Member) error

	// Retrieves the member with the given id.  Nil if the member doesn't exist.
	GetMember(cancel <-chan struct{}, id uuid.UUID) (Member, error)

	// Returns all of the currently active members.
	AllMembers(cancel <-chan struct{}) ([]Member, error)

	// Retrieves the member value with the given key and a flag indicating
	// whether or not the item exists.
	GetIndexValue(cancel <-chan struct{}, id uuid.UUID, key string) (val string, ver int, ok bool, err error)

	// Sets a global value using latest wins semantics.
	SetIndexValue(cancel <-chan struct{}, id uuid.UUID, key, val string, ver int) (ok bool, err error)

	// Sets a global value using latest wins semantics.
	DelIndexValue(cancel <-chan struct{}, id uuid.UUID, key, val string, ver int) (ok bool, err error)
}

// membership status
type Membership struct {
	Id      uuid.UUID
	Version int
	Active  bool
	since   time.Time
}

func (s Membership) String() string {
	var str string
	if s.Active {
		str = "Joined"
	} else {
		str = "Left" // really means gone
	}

	return fmt.Sprintf("%v(id=%v,ver=%v)", str, s.Id.String()[:8], s.Version)
}

type Health struct {
	Id      uuid.UUID
	Version int
	Healthy bool
	since   time.Time
}

func (h Health) String() string {
	var str string
	if h.Healthy {
		str = "Healthy"
	} else {
		str = "Unhealthy"
	}

	return fmt.Sprintf("%v(id=%v,ver=%v)", str, h.Id.String()[:8], h.Version)
}

type RosterListener interface {
	io.Closer
	Ctrl() common.Control
	Data() <-chan Membership
}

type HealthListener interface {
	io.Closer
	Ctrl() common.Control
	Data() <-chan Health
}

// An item in a store.
type Item struct {
	Val string
	Ver int
	Del bool
}

func defaultOptions(ctx common.Context) (*Options, error) {
	path := ctx.Config().Optional(Config.StoragePath, defaultStoragePath)

	db, err := stash.Open(ctx, path)
	if err != nil {
		return nil, errors.Wrapf(err, "Error opening db [%v]", path)
	}

	return &Options{
		Storage: db,
		Network: net.NewTcpNetwork(),
	}, nil
}

func buildOptions(ctx common.Context, fns []func(*Options)) (*Options, error) {
	opts, err := defaultOptions(ctx)
	if err != nil {
		return nil, err
	}

	for _, fn := range fns {
		fn(opts)
	}

	return opts, nil
}

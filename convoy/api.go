package convoy

import (
	"fmt"
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
	JoinError    = errors.New("Convoy:JoinError")
	ClosedError  = errors.New("Convoy:Closed")
)

type Options struct {
	Network net.Network
}

// Publishes the db to the given port.  This is the "first" member of the
// cluster and will not discover anyone else until it is contacted.
func Start(ctx common.Context, addr string, fns ...func(*Options)) (ret Host, err error) {
	opts := buildOptions(fns...)
	ret, err = newHost(ctx, opts.Network, addr, nil)
	return
}

func Join(ctx common.Context, addr string, peers []string, fns ...func(*Options)) (ret Host, err error) {
	opts := buildOptions(fns...)
	ret, err = newHost(ctx, opts.Network, addr, peers)
	return
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
	DelIndexValue(cancel <-chan struct{}, id uuid.UUID, key string, ver int) (ok bool, err error)
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

func buildOptions(fns ...func(*Options)) (ret *Options) {
	ret = &Options{
		Network: net.NewTcpNetwork(),
	}
	for _, fn := range fns {
		fn(ret)
	}
	return
}

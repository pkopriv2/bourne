package convoy

import (
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

	if host, err := newHost(ctx, db, opts.Network, addr, nil); err == nil {
		return host, err
	}

	return nil, err
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

	if host, err := newHost(ctx, db, opts.Network, addr, peers); err == nil {
		return host, err
	}

	return nil, err
}

// A host is the local member participating in and disseminating a shared
// directory.
type Host interface {
	io.Closer

	// The id of the host
	Id() uuid.UUID

	// The local member
	Self() (Member, error)

	// Provides access to the distributed directory.
	Directory() (Directory, error)

	// // The local store.
	// Store() Store

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

	// Returns a directory client.  Consumers are responsible for closing
	// the store once they are finished.
	Directory(net.Network, time.Duration) (Directory, error)

	// Returns a store client.  Consumers are responsible for closing
	// the store once they are finished.
	Store(net.Network, time.Duration) (Store, error)
}

// The directory is the central storage unit that hosts all information
// on all other members of the cluster.  All methods on the directory
// can fail, if the parent has closed or failed for any reason.
type Directory interface {
	io.Closer

	// Starts listening for joins. (currently only availabe for local directory)
	Joins() (Listener, error)

	// Starts listening for evictions. (currently only availabe for local directory)
	Evictions() (Listener, error)

	// Starts listening for failures. (currently only availabe for local directory)
	Failures() (Listener, error)

	// Retrieves the replica with the given id.  Nil if the member doesn't exist.
	Get(cancel <-chan struct{}, id uuid.UUID) (Member, error)

	// Returns all of the currently active members.
	All(cancel <-chan struct{}) ([]Member, error)

	// Evicts a member from the cluster.  The member will NOT automatically rejoin on eviction.
	Evict(cancel <-chan struct{}, m Member) error

	// Marks a member as being failed.  The next time the member contacts an
	// infected member, he will be forced to leave the cluster and rejoin.
	Fail(cancel <-chan struct{}, m Member) error
}

type Listener interface {
	io.Closer
	Ctrl() common.Control
	Data() <-chan uuid.UUID
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

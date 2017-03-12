package elmer

import (
	"io"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
)

var (
	PathError      = errors.New("Elmer:PathError")
	InvariantError = errors.New("Elmer:InvariantError")
)

func Start(ctx common.Context, self kayak.Host, addr string, opts ...func(*Options)) (Peer, error) {
	options, err := initOptions(ctx, opts)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return newPeer(ctx, self, addr, options)
}

func Connect(ctx common.Context, addrs []string, opts ...func(*Options)) (Client, error) {
	options, err := initOptions(ctx, opts)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return newPeerClient(ctx, options.net, options.rpcTimeout, options.rpcRosterRefresh, options.rpcClientPool, addrs), nil
}

type Client interface {
	io.Closer

	// Retrieves the root store.
	Root() (Store, error)

	// Retrieves the current roster
	Roster() ([]string, error)
}

type Peer interface {
	io.Closer

	// Retrieves the address of the peer.
	Addr() string

	// Retrieves the roster
	Roster() ([]string, error)

	// Retrieves the root store.
	Root() (Store, error)

	// Shuts the peer down.
	Shutdown() error
}

// A very simple key,value store abstraction. This store uses
// optimistic locking to provide a single thread-safe api for
// both local and remote stores.
type Store interface {
	io.Closer

	// The name of the store
	Name() []byte

	// Returns the store or nil if it doesn't exist.
	GetStore(cancel <-chan struct{}, name []byte) (Store, error)

	// Creates and returns the store
	NewStore(cancel <-chan struct{}, name []byte) (Store, error)

	// Deletes the nested store.
	DelStore(cancel <-chan struct{}, name []byte) error

	// Returns or creates the given store (if it doesn't exist)
	EnsureStore(cancel <-chan struct{}, name []byte) (Store, error)

	// All(cancel <-chan struct{}) ([]Item, error)

	// Returns the item and a flag indicating whether or not it exists.
	//
	// If the return value inclues an error, the other results should be disregarded
	Get(cancel <-chan struct{}, key []byte) (Item, bool, error)

	// Updates the value at the given key if the version matches. Returns the
	// item and a flag indicating whether or not the operation was successful
	// (ie the version matched), otherwise an error is returned.
	//
	// If the return value inclues an error, the other results should be disregarded
	Put(cancel <-chan struct{}, key []byte, val []byte, ver int) (Item, bool, error)

	// Deletes the value at the given key if the version matches. Returns a flag
	// indicating whether or not the operation was successful (ie the version matched),
	// otherwise an error is returned.
	//
	// If the return value inclues an error, the other results should be disregarded
	Del(cancel <-chan struct{}, key []byte, ver int) (bool, error)
}

func Update(cancel <-chan struct{}, store Store, key []byte, fn func([]byte) ([]byte, error)) (Item, error) {
	for {
		next, ok, err := TryUpdate(cancel, store, key, fn)
		if err != nil {
			return Item{}, errors.WithStack(err)
		}
		if ok {
			return next, nil
		}
	}
}

func TryUpdate(cancel <-chan struct{}, store Store, key []byte, fn func([]byte) ([]byte, error)) (Item, bool, error) {
	cur, ok, err := store.Get(cancel, key)
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	if !ok {
		cur = Item{key, nil, -1, false, 0}
	}

	new, err := fn(cur.Val)
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	next, ok, err := store.Put(cancel, key, new, cur.Ver)
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	return next, ok, nil
}


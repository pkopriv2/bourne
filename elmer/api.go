package elmer

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/scribe"
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

	// Retrieves the roster
	Roster() ([]string, error)

	// // Shuts the peer down.
	// Leave() error
//
	// // Shuts the peer down.
	// Shutdown(cause error) error
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

	// Creates a nested store if it doesn't exist.
	GetStore(cancel <-chan struct{}, name []byte) (Store, error)

	// Creates a nested store.
	CreateStore(cancel <-chan struct{}, name []byte) (Store, error)

	// Deletes the nested store.
	DeleteStore(cancel <-chan struct{}, name []byte) error

	// Ensures the given store is deleted.  If the store doesn't exist, an error is NOT returned.
	// All(cancel <-chan struct{}) ([]Item, error)

	// Returns the item or nil if it doesn't exist.
	//
	// If the return value inclues an error, the other results should
	// not be trusted.
	Get(cancel <-chan struct{}, key []byte) (Item, bool, error)

	// Updates the value at the given key if the version matches. Returns the
	// item and a flag indicating whether or not the operation was successful
	// (ie the version matched), otherwise an error is returned.
	//
	// If the return value inclues an error, the other results should
	// not be trusted.
	Put(cancel <-chan struct{}, key []byte, val []byte, ver int) (Item, bool, error)

	// Deletes the value at the given key if the version matches.
	// Returns a flag indicating whether or not the operation was
	// successful (ie the version matched), otherwise an error is
	// returned.
	//
	// If the return value inclues an error, the other results should
	// not be trusted.
	Del(cancel <-chan struct{}, key []byte, ver int) (bool, error)
}

// An item in a store.
type Item struct {
	Key []byte
	Val []byte
	Ver int
	Del bool
	seq int
	// ttl   time.Duration
}

func (i Item) String() string {
	return fmt.Sprintf("Item(key=%v,ver=%v): %v bytes", i.Key, i.Ver, len(i.Val))
}

func (i Item) Write(w scribe.Writer) {
	w.WriteBytes("key", i.Key)
	w.WriteBytes("val", i.Val)
	w.WriteInt("ver", i.Ver)
	w.WriteBool("del", i.Del)
	w.WriteInt("seq", i.seq)
}

func (i Item) Bytes() []byte {
	return scribe.Write(i).Bytes()
}

func readItem(r scribe.Reader) (item Item, err error) {
	err = common.Or(err, r.ReadBytes("key", &item.Key))
	err = common.Or(err, r.ReadBytes("val", &item.Val))
	err = common.Or(err, r.ReadInt("ver", &item.Ver))
	err = common.Or(err, r.ReadBool("del", &item.Del))
	err = common.Or(err, r.ReadInt("seq", &item.seq))
	return
}

func itemParser(r scribe.Reader) (interface{}, error) {
	return readItem(r)
}

func parseItemBytes(bytes []byte) (item Item, err error) {
	msg, err := scribe.Parse(bytes)
	if err != nil {
		return Item{}, err
	}

	return readItem(msg)
}

func Update(cancel <-chan struct{}, store Store, key []byte, fn func([]byte) ([]byte, error)) (Item, error) {
	for {
		cur, ok, err := store.Get(cancel, key)
		if err != nil {
			return Item{}, errors.WithStack(err)
		}

		if !ok {
			cur = Item{key, nil, -1, false, 0}
		}

		new, err := fn(cur.Val)
		if err != nil {
			return Item{}, errors.WithStack(err)
		}

		next, ok, err := store.Put(cancel, key, new, cur.Ver)
		if err != nil {
			return Item{}, errors.WithStack(err)
		}

		if ok {
			return next, nil
		}
	}
}

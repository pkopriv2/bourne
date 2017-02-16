package elmer

import (
	"bytes"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/scribe"
)

var (
	NoStoreError = errors.New("Elmer:NoStoreError")
)

func Start(ctx common.Context, self kayak.Host, opts ...func(*Options)) (Peer, error) {
	return nil, nil
}

func Join(ctx common.Context, self kayak.Host, addrs []string, opts ...func(*Options)) (Peer, error) {
	return nil, nil
}

func Connect(ctx common.Context, addrs []string, opts ...func(*Options)) (Peer, error) {
	options, err := buildOptions(ctx, opts)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return newPeerClient(ctx, options.Net, options.ConnTimeout, options.ConnPool, addrs), nil
}

type Peer interface {
	io.Closer

	// Retrieves the store catalog.
	Catalog() (Catalog, error)

	// Shuts the peer down.
	Shutdown() error
}

// The catalog of stores.
type Catalog interface {
	io.Closer

	// // Lists all the currently existing  stores.
	// All(cancel <-chan struct{}) ([]Store, error)

	// Ensures the given store is deleted.  If the store doesn't exist, an error is NOT returned.
	Del(cancel <-chan struct{}, store []byte) error

	// Retrieve the given store, or nil if it doesn't exist.
	Get(cancel <-chan struct{}, store []byte) (Store, error)

	// Ensures the given store exists and returns it.
	Ensure(cancel <-chan struct{}, store []byte) (Store, error)
}

// A very simple key,value store abstraction. This store uses
// optimistic locking to provide a single thread-safe api for
// both local and remote stores.
type Store interface {
	io.Closer

	// The name of the store
	Name() []byte

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
	Store []byte
	Key   []byte
	Val   []byte
	Ver   int
	// Ttl   time.Duration
}

func (i Item) String() string {
	return fmt.Sprintf("Item(store=%v,key=%v,ver=%v): %v bytes", string(i.Store), i.Key, i.Ver, len(i.Val))
}

func (i Item) Write(w scribe.Writer) {
	w.WriteBytes("store", i.Store)
	w.WriteBytes("key", i.Key)
	w.WriteBytes("val", i.Val)
	w.WriteInt("ver", i.Ver)
}

func (i Item) Bytes() []byte {
	return scribe.Write(i).Bytes()
}

func (i Item) Equal(o Item) bool {
	if i.Ver != o.Ver {
		return false
	}

	if !bytes.Equal(i.Store, o.Store) {
		return false
	}

	if !bytes.Equal(i.Key, o.Key) {
		return false
	}

	return bytes.Equal(i.Val, o.Val)
}

func readItem(r scribe.Reader) (item Item, err error) {
	err = common.Or(err, r.ReadBytes("store", &item.Store))
	err = common.Or(err, r.ReadBytes("key", &item.Key))
	err = common.Or(err, r.ReadBytes("val", &item.Val))
	err = common.Or(err, r.ReadInt("ver", &item.Ver))
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

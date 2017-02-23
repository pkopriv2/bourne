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
	InvariantError = errors.New("Elmer:InvariantError")
	PathError      = errors.New("Elmer:PathError")
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

func (i Item) Equal(o Item) bool {
	return i.Ver == o.Ver &&
		i.Del == o.Del &&
		bytes.Equal(i.Key, o.Key) &&
		bytes.Equal(i.Val, o.Val)
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

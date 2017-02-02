package elmer

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/scribe"
)

// An item in a store.
type Item struct {
	Key []byte
	Val []byte
	Ver int
}

func readItem(r scribe.Reader) (item Item, err error) {
	err = common.Or(err, r.ReadBytes("key", &item.Key))
	err = common.Or(err, r.ReadBytes("val", &item.Val))
	err = common.Or(err, r.ReadInt("ver", &item.Ver))
	return
}

func parseItemBytes(bytes []byte) (item Item, err error) {
	msg, err := scribe.Parse(bytes)
	if err != nil {
		return Item{}, err
	}

	return readItem(msg)
}

func (i Item) Write(w scribe.Writer) {
	w.WriteBytes("key", i.Key)
	w.WriteBytes("val", i.Val)
	w.WriteInt("ver", i.Ver)
}

func (i Item) String() string {
	return fmt.Sprintf("(%v,%v)", i.Key, i.Ver)
}

func Start(ctx common.Context, listener net.Listener, self kayak.Peer) (Store, error) {
	return nil, nil
}

func Join(ctx common.Context, listener net.Listener, self kayak.Peer, addrs []string) (Store, error) {
	return nil, nil
}

func Connect(ctx common.Context, addrs []string) (Store, error) {
	return nil, nil
}

// A very simple key,value store abstraction. This store uses
// optimistic locking to provide a single thread-safe api for
// both local and remote stores.
type Store interface {
	io.Closer

	// Returns the item or nil if it doesn't exist.
	//
	// If the return value inclues an error, the other results should
	// not be trusted.
	Get(cancel <-chan struct{}, key []byte) (Item, bool, error)

	// Updates the value at the given key if the version matches.
	// Returns a flag indicating whether or not the operation was
	// successful (ie the version matched), otherwise an error is
	// returned.
	//
	// If the return value inclues an error, the other results should
	// not be trusted.
	Put(cancel <-chan struct{}, key []byte, val []byte, prev int) (Item, bool, error)

	// Deletes the value at the given key if the version matches.
	// Returns a flag indicating whether or not the operation was
	// successful (ie the version matched), otherwise an error is
	// returned.
	//
	// If the return value inclues an error, the other results should
	// not be trusted.
	Del(timeout time.Duration, key []byte, prev int) (bool, error)
}

package elmer

import (
	"bytes"
	"fmt"
	"io"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/scribe"
)

func Start(ctx common.Context, self kayak.Host, opts ...func(*Options)) (Peer, error) {
	return nil, nil
}

func Join(ctx common.Context, self kayak.Host, addrs []string, opts ...func(*Options)) (Peer, error) {
	return nil, nil
}

func Connect(ctx common.Context, addrs []string, opts ...func(*Options)) (Store, error) {
	return nil, nil
}

type Peer interface {
	io.Closer
	Store() (Store, error)
	Shutdown() error
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

	// Updates the value at the given key if the version matches. Returns the
	// item and a flag indicating whether or not the operation was successful
	// (ie the version matched), otherwise an error is returned.
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
	Del(cancel <-chan struct{}, key []byte, prev int) (bool, error)
}

// An item in a store.
type Item struct {
	Key  []byte
	Val  []byte
	Prev int
}

func (i Item) Write(w scribe.Writer) {
	w.WriteBytes("key", i.Key)
	w.WriteBytes("val", i.Val)
	w.WriteInt("prev", i.Prev)
}

func (i Item) Bytes() []byte {
	return scribe.Write(i).Bytes()
}

func (i Item) Equal(o Item) bool {
	if i.Prev != o.Prev {
		return false
	}

	if ! bytes.Equal(i.Key, o.Key) {
		return false
	}

	return bytes.Equal(i.Val, o.Val)
}

func (i Item) String() string {
	return fmt.Sprintf("(%v,%v)", i.Key, i.Prev)
}

func readItem(r scribe.Reader) (item Item, err error) {
	err = common.Or(err, r.ReadBytes("key", &item.Key))
	err = common.Or(err, r.ReadBytes("val", &item.Val))
	err = common.Or(err, r.ReadInt("prev", &item.Prev))
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

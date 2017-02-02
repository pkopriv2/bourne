package elmer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/scribe"
)

var (
	ClosedError   = errors.New("Elmer:ClosedError")
	TimeoutError  = errors.New("Elmer:TimeoutError")
	CanceledError = errors.New("Elmer:CanceledError")
)

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
	if eq := i.Prev == o.Prev; !eq {
		return false
	}

	if eq := bytes.Equal(i.Key, o.Key); !eq {
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

func parseItemBytes(bytes []byte) (item Item, err error) {
	msg, err := scribe.Parse(bytes)
	if err != nil {
		return Item{}, err
	}

	return readItem(msg)
}

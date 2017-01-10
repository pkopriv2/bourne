package kayak

import (
	"fmt"
	"io"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
)

// An item in a store.
type Item struct {
	Key []byte
	Val []byte
	Ver int

	// internal only
	time time.Time
}

func readItem(r scribe.Reader) (item Item, err error) {
	err = common.Or(err, r.ReadBytes("key", &item.Key))
	err = common.Or(err, r.ReadBytes("val", &item.Val))
	err = common.Or(err, r.ReadInt("ver", &item.Ver))
	return
}

func (i Item) Write(w scribe.Writer) {
	w.WriteBytes("key", i.Key)
	w.WriteBytes("val", i.Val)
	w.WriteInt("ver", i.Ver)
}

func (i Item) String() string {
	return fmt.Sprintf("(%v,%v)", i.Key, i.Ver)
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

	// Returns the item or nil if it doesn't exist.
	//
	// If the return value inclues an error, the other results should
	// not be trusted.
	Get(key []byte) (bool, error)

	// Updates the value at the given key if the version matches.
	// Returns a flag indicating whether or not the operation was
	// successful (ie the version matched) and if so, the updated
	// value.  Otherwise an error is returned.
	//
	// If the return value inclues an error, the other results should
	// not be trusted.
	Put(key []byte, val []byte, prev int) (bool, error)

	// Deletes the value at the given key if the version matches.
	// Returns a flag indicating whether or not the operation was
	// successful (ie the version matched) and if so, the updated
	// value.  Otherwise an error is returned.
	//
	// If the return value inclues an error, the other results should
	// not be trusted.
	Del(key []byte, prev int) (bool, error)
}

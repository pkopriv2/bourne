package kayak

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/pkopriv2/bourne/scribe"
)

// Public Error Types
var (
	EvictedError   = errors.New("Kayak:Evicted")
	FailedError    = errors.New("Kayak:Failed")
	ClosedError    = errors.New("Kayak:Closed")
	NotLeaderError = errors.New("Kayak:NotLeader")
	NoLeaderError  = errors.New("Kayak:NoLeader")
)

type TimeoutError struct {
	timeout time.Duration
	msg     string
}

func NewTimeoutError(timeout time.Duration, msg string) TimeoutError {
	return TimeoutError{timeout, msg}
}

func (t TimeoutError) Error() string {
	return fmt.Sprintf("Timeout[%v]: %v", t.timeout, t.msg)
}

type event interface {
	scribe.Writable
}

//
type Parser func(scribe.Reader) (event, error)

// A host is the local member participating in and disseminating a shared
// directory.
type Host interface {
	io.Closer

	// The local store.
	Store() Store
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
	Get(key []byte) (item *Item, err error)

	// Updates the value at the given key if the version matches.
	// Returns a flag indicating whether or not the operation was
	// successful (ie the version matched) and if so, the updated
	// value.  Otherwise an error is returned.
	//
	// If the return value inclues an error, the other results should
	// not be trusted.
	Put(key []byte, val []byte, ttl time.Duration, expected int) (*Item, error)

	// Deletes the value at the given key if the version matches.
	// Returns a flag indicating whether or not the operation was
	// successful (ie the version matched) and if so, the updated
	// value.  Otherwise an error is returned.
	//
	// If the return value inclues an error, the other results should
	// not be trusted.
	Del(key []byte, expected int) (*Item, error)

	Listen(key []byte) Listener
}

// An item in a store.
type Listener interface {
	io.Closer
	Updates() <-chan Item
}

// An item in a store.
type Item struct {
	Val  []byte
	Ver  int
	Ttl  time.Duration
	Time time.Time
}

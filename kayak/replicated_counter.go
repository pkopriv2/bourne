package kayak

import (
	"sync"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/scribe"
)

type ReplicatedCounter interface {
	// Inc() (int, error)
	// Dec() (int, error)
	Get() int
	Swap(int, int) (bool, error)
}

type swapEvent struct {
	Prev int
	Next int
}

func (c swapEvent) Write(w scribe.Writer) {
	w.WriteInt("prev", c.Prev)
	w.WriteInt("next", c.Next)
}

func swapParser(r scribe.Reader) (Event, error) {
	var evt swapEvent
	var err error

	err = common.Or(err, r.ReadInt("prev", &evt.Prev))
	err = common.Or(err, r.ReadInt("next", &evt.Next))
	return evt, err
}

type swapResponse struct {
	success bool
	err     error
}

type counterUpdate struct {
	swapEvent
	ack chan swapResponse
}

func newCounterUpdate(e swapEvent) *counterUpdate {
	return &counterUpdate{e, make(chan swapResponse, 1)}
}

func (c *counterUpdate) reply(success bool, err error) {
	c.ack <- swapResponse{success, err}
}

type counterValue struct {
	raw   int
	index int
}

type counter struct {
	ctx common.Context

	value     counterValue
	valueLock sync.Mutex

	updates     chan *counterUpdate
	requestPool concurrent.WorkPool
	closed      chan struct{}
	closer      chan struct{}
}

func (r *counter) Close() error {
	// r.host.Close()
	close(r.closed)
	return nil
}

func (c *counter) Get() int {
	return c.val().raw
}

func (c *counter) Context() common.Context {
	return c.ctx
}

func (c *counter) Parser() Parser {
	return swapParser
}

func (c *counter) Swap(e int, a int) (bool, error) {
	update := newCounterUpdate(swapEvent{e, a})
	select {
	case <-c.closed:
		return false, ClosedError
	case c.updates<-update:
		select {
		case <-c.closed:
			return false, ClosedError
		case r := <-update.ack:
			return r.success, r.err
		}
	}
}

func (c *counter) Snapshot() ([]Event, error) {
	return []Event{swapEvent{0, c.Get()}}, nil
}

func (c *counter) Run(log MachineLog) {
	go func() {
		for {
		}
	}()

}

func (c *counter) startAppender() {
	// go func() {
		// for req := range ch {
//
		// }
	// }()
}

func (c *counter) RunProxy(ch <-chan AppendRequest) {
	// go func() {
		// for req := range ch {
//
		// }
	// }()
}

func (c *counter) val() counterValue {
	c.valueLock.Lock()
	defer c.valueLock.Unlock()
	return c.value
}

func (c *counter) swap(cur counterValue, new counterValue) bool {
	c.valueLock.Lock()
	defer c.valueLock.Unlock()
	if c.value == cur {
		return false
	}

	c.value = new
	return true
}




func NewReplicatedCounter(ctx common.Context, port int, peers []string) (ReplicatedCounter, error) {
	return nil, nil
}

// // An item in a store.
// type Item struct {
// Key []byte
// Val []byte
// Ver int
//
// // internal only
// time time.Time
// }
//
// func readItem(r scribe.Reader) (item Item, err error) {
// err = common.Or(err, r.ReadBytes("key", &item.Key))
// err = common.Or(err, r.ReadBytes("val", &item.Val))
// err = common.Or(err, r.ReadInt("ver", &item.Ver))
// return
// }
//
// func (i Item) Write(w scribe.Writer) {
// w.WriteBytes("key", i.Key)
// w.WriteBytes("val", i.Val)
// w.WriteInt("ver", i.Ver)
// }
//
// func (i Item) String() string {
// return fmt.Sprintf("(%v,%v)")
// }

// // A host is the local member participating in and disseminating a shared
// // directory.
// type Host interface {
// io.Closer
//
// // The local store.
// Store() Store
// }
//
// // A very simple key,value store abstraction. This store uses
// // optimistic locking to provide a single thread-safe api for
// // both local and remote stores.
// //
// // If this is the local store, closing the store will NOT disconnect
// // the replica, it simply prevents any changes to the store from
// // occurring.
// type Store interface {
// io.Closer
//
// // Returns the item or nil if it doesn't exist.
// //
// // If the return value inclues an error, the other results should
// // not be trusted.
// Get(key []byte) (bool, error)
//
// // Updates the value at the given key if the version matches.
// // Returns a flag indicating whether or not the operation was
// // successful (ie the version matched) and if so, the updated
// // value.  Otherwise an error is returned.
// //
// // If the return value inclues an error, the other results should
// // not be trusted.
// Put(key []byte, val []byte, prev int) (bool, error)
//
// // Deletes the value at the given key if the version matches.
// // Returns a flag indicating whether or not the operation was
// // successful (ie the version matched) and if so, the updated
// // value.  Otherwise an error is returned.
// //
// // If the return value inclues an error, the other results should
// // not be trusted.
// Del(key []byte, prev int) (bool, error)
// }

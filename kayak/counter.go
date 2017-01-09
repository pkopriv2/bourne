package kayak

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
)

type ReplicatedCounter interface {
	Inc() (int, error)
	Dec() (int, error)
	Get() (int, error)
}

type counterEvent struct {
	Prev int
	Inc  bool
}

func (c counterEvent) Write(w scribe.Writer) {
	w.WriteInt("prev", c.Prev)
	w.WriteBool("inc", c.Inc)
}

func counterParser(r scribe.Reader) (Event, error) {
	var evt counterEvent
	var err error
	err = r.ReadBool("inc", &evt.Inc)
	err = common.Or(err, r.ReadInt("prev", &evt.Prev))
	return evt, err
}

type replicatedCounter struct {
	value int
	in    chan Event
}

func (r *replicatedCounter) Close() error {
	panic("not implemented")
}

type replicatedCounterMachine struct {
	value int
}

func (r *replicatedCounterMachine) Close() error {
	panic("not implemented")
}

func (r *replicatedCounterMachine) Snapshot() []Event {
	panic("not implemented")
}

func (r *replicatedCounterMachine) Handle(Event) bool {
	panic("not implemented")
}

func NewReplicatedCounter(peers []string) (*replicatedCounter, error) {
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

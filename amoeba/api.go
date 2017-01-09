package amoeba

import "time"

// The core indexing abstraction and implementation.
//
// This indexer is designed for an eventually convergent,
// distributed roster.  The eventing abstraction is inteneded to
// be the primary mechanism of distribution. Because of its role
// in a distributed datase, more focus was spent on correctness
// than the performance of the implementation.  As a naive, first
// attempt, each indexer will be managed by a global lock.  I think
// a good next move would be to move to a persistent btree
// implementation.  The hope is that the current abstraction
// is expressive enough to support such a change without major changes
// to the interfaces.

// A generic key type.  Keys must be comparable and hashable.
type Key interface {
	Compare(Key) int
	Hash() string
}

// Generic scanning abstraction.   Allows for halting and skipping
type Scan interface {

	// Communicates to the executing scan to move to the item
	// that is the smallest node greater than or equal to the
	// input key.  If no such node exists, then this behaves
	// equivalently to when stop is called.
	Next(Key)

	// Communicates to the executing scan to halt and stop
	// further execution.  Once the consumer has returned
	// from the scan function, the scan will return.
	Stop()
}

// The core storage object.  Basically just manages read/write transactions
// over the underlying index.
type Index interface {

	// Returns the number of items in the index
	Size() int

	// Performs a read on the indexer. The indexer supports miltiple reader,
	// single writer semantics.
	Read(func(View))

	// Performs an update on the indexer. The indexer supports miltiple reader,
	// single writer semantics.
	Update(func(Update))
}

// Access methods for an index.
type View interface {
	// the start time of the transaction. (consistent throughout the transaction)
	Time() time.Time

	// Retrieves the item for the given key.  Nil if it doesn't exist.
	Get(key Key) interface{}

	// Scans the index starting with the minimum node.
	Scan(fn func(Scan, Key, interface{}))

	// Scans beginning at the first node that is greater than or equal to the input key.
	ScanFrom(start Key, fn func(Scan, Key, interface{}))
}

// Update methods for an index.
type Update interface {
	View

	// Puts the value at the key.
	Put(key Key, val interface{})

	// Deletes the key.
	Del(key Key)
}

// Utility functions
func Get(idx Index, key Key) (ret interface{}) {
	idx.Read(func(v View) {
		ret = v.Get(key)
	})
	return
}

func Put(idx Index, key Key, val interface{}) {
	idx.Update(func(u Update) {
		u.Put(key, val)
	})
}

func Del(idx Index, key Key) {
	idx.Update(func(u Update) {
		u.Del(key)
	})
}

func ScanAll(idx Index, fn func(Scan, Key, interface{})) {
	idx.Read(func(u View) {
		u.Scan(func(s Scan, k Key, v interface{}) {
			fn(s, k, v)
		})
	})
}

func ScanFrom(idx Index, start Key, fn func(Scan, Key, interface{})) {
	idx.Read(func(u View) {
		u.ScanFrom(start, func(s Scan, k Key, v interface{}) {
			fn(s, k, v)
		})
	})
}

// scan impl.
type scan struct {
	next Key
	stop bool
}

func (s *scan) Next(i Key) {
	s.next = i
}

func (s *scan) Stop() {
	s.stop = true
}

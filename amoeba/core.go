package amoeba

import (
	"sync"

	"github.com/pkopriv2/bourne/btree"
)

type Sortable interface {
	Compare(Sortable) int
}

// A generic key type.  The only requirement of keys is they sortable
type Key interface {
	Sortable
}

// A scan gives consumers influence over scanning behavior.
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
type RawIndex interface {

	// Returns the number of items in the index
	Size() int

	// Performs a read on the indexer. The indexer supports miltiple reader,
	// single writer semantics.
	Read(func(RawView))

	// Performs an update on the indexer. The indexer supports miltiple reader,
	// single writer semantics.
	Update(func(RawUpdate))
}

// Access methods for an index.
type RawView interface {

	// Retrieves the item for the given key.  Nil if it doesn't exist.
	Get(key Key) interface{}

	// Scans the index starting with the minimum node.
	Scan(fn func(Scan, Key, interface{}))

	// Scans beginning at the first node that is greater than or equal to the input key.
	ScanFrom(start Key, fn func(Scan, Key, interface{}))
}

// Update methods for an index.
type RawUpdate interface {
	RawView

	// Puts the value at the key.
	Put(key Key, val interface{})

	// Deletes the key.
	Del(key Key)
}

// Utility functions
func RawGet(idx RawIndex, key Key) (ret interface{}) {
	idx.Read(func(v RawView) {
		ret = v.Get(key)
	})
	return
}

func RawPut(idx RawIndex, key Key, val interface{}) {
	idx.Update(func(u RawUpdate) {
		u.Put(key, val)
	})
}

func RawDel(idx RawIndex, key Key) {
	idx.Update(func(u RawUpdate) {
		u.Del(key)
	})
}

func RawScan(idx RawIndex, fn func(Scan, Key, interface{})) {
	idx.Read(func(u RawView) {
		u.Scan(func(s Scan, k Key, v interface{}) {
			fn(s, k, v)
		})
	})
}

func RawScanFrom(idx RawIndex, start Key, fn func(Scan, Key, interface{})) {
	idx.Read(func(u RawView) {
		u.ScanFrom(start, func(s Scan, k Key, v interface{}) {
			fn(s, k, v)
		})
	})
}

// Scan implementation
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

// Adapts a consumer key to a btree item
type indexKey struct {
	Key Key
}

func (i indexKey) Compare(o Sortable) int {
	return i.Key.Compare(o.(indexKey).Key)
}

func (i indexKey) Less(than btree.Item) bool {
	return i.Compare(than.(indexKey)) < 0
}

// the index implementation.
type btreeIndex struct {
	Tree  *btree.BTree
	Table map[Key]interface{}
	Lock  sync.RWMutex
}

func NewBTreeIndex(degree int) RawIndex {
	return &btreeIndex{
		Tree:  btree.New(degree),
		Table: make(map[Key]interface{})}
}

func (r *btreeIndex) Size() int {
	r.Lock.RLock()
	defer r.Lock.RUnlock()
	return len(r.Table)
}

func (r *btreeIndex) Read(fn func(RawView)) {
	r.Lock.RLock()
	defer r.Lock.RUnlock()
	fn(r)
}

func (r *btreeIndex) Update(fn func(RawUpdate)) {
	r.Lock.Lock()
	defer r.Lock.Unlock()
	fn(r)
}

// Note: All following assume a lock has been taken
func (i *btreeIndex) Put(key Key, val interface{}) {
	i.Table[key] = val
	i.Tree.ReplaceOrInsert(indexKey{key})
}

func (i *btreeIndex) Del(key Key) {
	delete(i.Table, key)
	i.Tree.Delete(indexKey{key})
}

func (i *btreeIndex) Get(key Key) interface{} {
	return i.Table[key]
}

func (r *btreeIndex) Scan(fn func(Scan, Key, interface{})) {
	if r.Tree.Len() == 0 {
		return
	}

	r.ScanFrom(r.Tree.Min().(indexKey).Key, fn)
}

func (r *btreeIndex) ScanFrom(start Key, fn func(Scan, Key, interface{})) {
	next := indexKey{start}
	for {
		if r.Tree.Len() == 0 {
			return
		}

		if r.Tree.Max().Less(next) {
			return
		}

		scan := &scan{}
		r.Tree.AscendGreaterOrEqual(next, func(i btree.Item) bool {
			key := i.(indexKey).Key
			val := r.Table[key]
			fn(scan, key, val)
			return !scan.stop && scan.next == nil
		})

		if scan.stop || scan.next == nil {
			return
		}

		next = indexKey{scan.next}
	}
}

package amoeba

import (
	"sync"

	"github.com/pkopriv2/bourne/btree"
)

// Adapts a consumer key to a btree item
type btreeKey struct {
	Key Key
}

func (i btreeKey) Compare(o Key) int {
	return i.Key.Compare(o.(btreeKey).Key)
}

func (i btreeKey) Less(than btree.Item) bool {
	return i.Compare(than.(btreeKey)) < 0
}

// the index implementation.
type btreeIndex struct {
	Tree  *btree.BTree
	Table map[Key]interface{}
	Lock  sync.RWMutex
}

func NewBTreeIndex(degree int) Index {
	return &btreeIndex{
		Tree:  btree.New(degree),
		Table: make(map[Key]interface{})}
}

func (r *btreeIndex) Size() int {
	r.Lock.RLock()
	defer r.Lock.RUnlock()
	return len(r.Table)
}

func (r *btreeIndex) Read(fn func(View)) {
	r.Lock.RLock()
	defer r.Lock.RUnlock()
	fn(r)
}

func (r *btreeIndex) Update(fn func(Update)) {
	r.Lock.Lock()
	defer r.Lock.Unlock()
	fn(r)
}

// Note: All following assume a lock has been taken.  These are NOT part
// of the public api, except in the context of an update or read...
func (i *btreeIndex) Put(key Key, val interface{}) {
	i.Table[key] = val
	i.Tree.ReplaceOrInsert(btreeKey{key})
}

func (i *btreeIndex) Del(key Key) {
	delete(i.Table, key)
	i.Tree.Delete(btreeKey{key})
}

func (i *btreeIndex) Get(key Key) interface{} {
	return i.Table[key]
}

func (r *btreeIndex) Scan(fn func(Scan, Key, interface{})) {
	if r.Tree.Len() == 0 {
		return
	}

	r.ScanFrom(r.Tree.Min().(btreeKey).Key, fn)
}

func (r *btreeIndex) ScanFrom(start Key, fn func(Scan, Key, interface{})) {
	next := btreeKey{start}
	for {
		if r.Tree.Len() == 0 {
			return
		}

		if r.Tree.Max().Less(next) {
			return
		}

		scan := &scan{}
		r.Tree.AscendGreaterOrEqual(next, func(i btree.Item) bool {
			key := i.(btreeKey).Key
			val := r.Table[key]
			fn(scan, key, val)
			return !scan.stop && scan.next == nil
		})

		if scan.stop || scan.next == nil {
			return
		}

		next = btreeKey{scan.next}
	}
}

package amoeba

import (
	"sync"
	"time"

	"github.com/pkopriv2/bourne/btree"
)

// Adapts a consumer key to a btree item
type btreeKey struct {
	Key Key
}

func (i btreeKey) Hash() string {
	return i.Key.Hash()
}

func (i btreeKey) Compare(o Key) int {
	return i.Key.Compare(o.(btreeKey).Key)
}

func (i btreeKey) Less(than btree.Item) bool {
	return i.Compare(than.(btreeKey)) < 0
}

type btreeView struct {
	idx *btreeIndex
	now time.Time
}

func (b *btreeView) Time() time.Time {
	return b.now
}

func (b *btreeView) Get(key Key) interface{} {
	return b.idx.Get(key)
}

func (b *btreeView) Scan(fn func(Scan, Key, interface{})) {
	b.idx.Scan(fn)
}

func (b *btreeView) ScanFrom(start Key, fn func(Scan, Key, interface{})) {
	b.idx.ScanFrom(start, fn)
}

type btreeUpdate struct {
	*btreeView
}

func (b *btreeUpdate) Put(key Key, val interface{}) {
	b.idx.Put(key, val)
}

func (b *btreeUpdate) Del(key Key) {
	b.idx.Del(key)
}

// the index implementation.
type btreeIndex struct {
	Tree  *btree.BTree
	Table map[string]interface{}
	Lock  sync.RWMutex
}

func NewBTreeIndex(degree int) Index {
	return &btreeIndex{
		Tree:  btree.New(degree),
		Table: make(map[string]interface{})}
}

func (r *btreeIndex) Size() int {
	r.Lock.RLock()
	defer r.Lock.RUnlock()
	return len(r.Table)
}

func (r *btreeIndex) Read(fn func(View)) {
	r.Lock.RLock()
	defer r.Lock.RUnlock()
	fn(&btreeView{r, time.Now()})
}

func (r *btreeIndex) Update(fn func(Update)) {
	r.Lock.Lock()
	defer r.Lock.Unlock()
	fn(&btreeUpdate{&btreeView{r, time.Now()}})
}

// Note: All following assume a lock has been taken.  These are NOT part
// of the public api, except in the context of an update or read...
func (i *btreeIndex) Put(key Key, val interface{}) {
	i.Table[key.Hash()] = val
	i.Tree.ReplaceOrInsert(btreeKey{key})
}

func (i *btreeIndex) Del(key Key) {
	delete(i.Table, key.Hash())
	i.Tree.Delete(btreeKey{key})
}

func (i *btreeIndex) Get(key Key) interface{} {
	return i.Table[key.Hash()]
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
			val := r.Table[key.Hash()]
			fn(scan, key, val)
			return !scan.stop && scan.next == nil
		})

		if scan.stop || scan.next == nil {
			return
		}

		next = btreeKey{scan.next}
	}
}

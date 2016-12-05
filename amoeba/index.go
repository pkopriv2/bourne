package amoeba

import (
	"time"

	"github.com/pkopriv2/bourne/btree"
)

// the index value
type item struct {
	val  Val
	ver  int
	Time time.Time
}

func (i item) Val() Val {
	return i.val
}

func (i item) Ver() int {
	return i.ver
}

// The btree value.  In this case, we're just mantaining a specific sort.
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
type index struct {
	tree   *btree.BTree
	table  map[btree.Item]item
	closed chan struct{}
	closer chan struct{}
}

func newIndex(degree int) *index {
	idx := &index{
		tree:  btree.New(degree),
		table: make(map[btree.Item]item)}

	return idx
}

func (i *index) Size() int {
	return len(i.table)
}

func (i *index) Put(key Key, val Val, ver int, time time.Time) bool {
	indexKey := indexKey{key}
	item := item{val, ver, time}

	cur, ok := i.table[indexKey]
	if !ok {
		i.table[indexKey] = item
		i.tree.ReplaceOrInsert(indexKey)
		return true
	}

	if cur.Ver() < ver {
		i.table[indexKey] = item
		i.tree.ReplaceOrInsert(indexKey)
		return true
	}

	return false
}

func (i *index) DelNow(key Key) {
	item := indexKey{key}
	i.tree.Delete(item)
	delete(i.table, item)
}

func (i *index) Del(key Key, ver int, time time.Time) bool {
	indexKey := indexKey{key}
	item := item{nil, ver, time}

	cur, ok := i.table[indexKey]
	if !ok {
		i.table[indexKey] = item
		i.tree.ReplaceOrInsert(indexKey)
		return true
	}

	if cur.Ver() <= ver {
		i.table[indexKey] = item
		i.tree.ReplaceOrInsert(indexKey)
		return true
	}

	return false
}

func (i *index) Get(key Key) Item {
	val, ok := i.table[indexKey{key}]
	if !ok {
		return nil
	}

	return val
}

func (r *index) Scan(fn func(*Scan, Key, Item)) {
	if r.tree.Len() == 0 {
		return
	}

	r.ScanFrom(r.tree.Min().(indexKey).Key, fn)
}

func (r *index) ScanFrom(start Key, fn func(*Scan, Key, Item)) {
	next := indexKey{start}
	for {
		if r.tree.Len() == 0 {
			return
		}

		if r.tree.Max().Less(next) {
			return
		}

		scan := &Scan{}
		r.tree.AscendGreaterOrEqual(next, func(i btree.Item) bool {
			key := i.(indexKey)
			item := r.table[key]
			fn(scan, key.Key, item)
			return !scan.stop && scan.next == nil
		})

		if scan.stop || scan.next == nil {
			return
		}

		next = indexKey{scan.next}
	}
}

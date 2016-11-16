package amoeba

import (
	"time"

	"github.com/pkopriv2/bourne/btree"
)

// An index stores arbitrary data in a sorted, searchable
// fashion.  In reality, it is simply a btree of keys with
// a lookup table from key to value.  This implements
// hash-like put/get/delete operations which all perform
// in logb(n).  It also supports primitive scan facilities
// which can be used to search arbitrary subsections of
// the index.
//
// The original intention of this indexing solution is to
// be used in an eventually convergent database.  Therefore,
// every data event must be versioned.



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
	tree  *btree.BTree
	table map[btree.Item]item
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

func (i *index) Put(key Key, val Val, ver int, time time.Time) {
	indexKey := indexKey{key}
	item := item{val, ver, time}

	cur, ok := i.table[indexKey]
	if !ok {
		i.table[indexKey] = item
		i.tree.ReplaceOrInsert(indexKey)
		return
	}

	if cur.Ver() <= ver {
		i.table[indexKey] = item
		i.tree.ReplaceOrInsert(indexKey)
		return
	}
}

func (i *index) Del(key Key, ver int, time time.Time) {
	i.Put(key, nil, ver, time)
}

func (i *index) Get(key Key) Item {
	val, ok := i.table[indexKey{key}]
	if !ok {
		return nil
	}

	if val.Val() == nil {
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
		if r.tree.Max().Less(next) {
			return
		}

		scan := &Scan{}
		r.tree.AscendGreaterOrEqual(next, func(i btree.Item) bool {
			key := i.(indexKey)
			item := r.table[key]
			if item.Val == nil {
				return true
			}

			fn(scan, key.Key, item)
			return !scan.stop && scan.next == nil
		})

		if scan.stop || scan.next == nil {
			return
		}

		next = indexKey{scan.next}
	}
}

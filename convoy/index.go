package convoy

import "github.com/pkopriv2/bourne/btree"

// An index stores arbitrary data in a sorted, searchable
// fashion.  In reality, it is simply a btree of keys with
// a lookup table from key to value.  This implements
// hash-like put/get/delete operations which all perform
// in log32(n).  It also supports primitive scan facilities
// which can be used to search arbitrary subsections of
// the index.

// An index key is an element in the backing tree.
type indexKey interface {
	btree.Item
}

// The primary scan abstraction.  It is called for each key
// that is encountered during the scan.  Consumers may choose
// to cancel a scan by invoking s.Stop() and returning.
type indexScanner func(s *indexScan, k indexKey)

// The scan gives consumers influence over the scan.
type indexScan struct {
	next indexKey
	stop bool
}

// Communicates to the executing scan to move to the item
// that is the smallest node greater than or equal to the
// input key.  If no such node exists, then this behaves
// equivalently to when stop is called.
func (s *indexScan) Next(i indexKey) {
	s.next = i
}

// Communicates to the executing scan to halt and stop
// further execution.  Once the consumer has returned
// from the scanner, the scan will return.
func (s *indexScan) Stop() {
	s.stop = true
}

// the index implementation.
type index struct {
	table map[btree.Item]interface{}
	tree  *btree.BTree
}

func newIndex() *index {
	return &index{make(map[btree.Item]interface{}), btree.New(32)}
}

func (r *index) Size() int {
	return len(r.table)
}

func (r *index) Put(key indexKey, val interface{}) {
	r.tree.ReplaceOrInsert(key)
	r.table[key] = val
}

func (r *index) Remove(key indexKey) {
	r.tree.Delete(key)
	delete(r.table, key)
}

func (r *index) Get(key indexKey) interface{} {
	item := r.tree.Get(key)
	if item == nil {
		return nil
	}

	return r.table[key]
}

func (r *index) Scan(fn indexScanner) {
	r.ScanAt(r.tree.Min(), fn)
}

func (r *index) ScanAt(start indexKey, fn indexScanner) {
	next := start
	for {
		if next == nil {
			return
		}

		if r.tree.Max().Less(next) {
			return
		}

		indexScan := &indexScan{}
		r.tree.AscendGreaterOrEqual(next, func(i btree.Item) bool {
			fn(indexScan, i)
			next = indexScan.next
			return !indexScan.stop && next == nil
		})

		if indexScan.stop {
			return
		}
	}
}

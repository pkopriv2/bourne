package amoeba

import (
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

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

// Basic index item.  To support automatic gc'ing, we need to know how
// old an item is and whether or not it has been deleted.
type Item interface {

	// The version of the item
	Ver() int

	// Whether or not this item was deleted.
	Del() bool

	// The raw value of the item
	Val() interface{}

	// The transaction time when then item was created.
	Time() time.Time
}

// The core storage object.  Basically just manages read/write transactions
// over the underlying index.
type Index interface {
	io.Closer

	// Returns the size of the index
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

	// Current view time (consistent for the lifetime of the view)
	Time() time.Time

	// Retrieves the item for the given key.  Nil if it doesn't exist.
	Get(key Key) Item

	// Scans all items in the index - even those that are candidates for gc.
	Scan(fn func(Scan, Key, Item))

	// Scans beginning at the first node that is greater than or
	// equal to the input key.
	ScanFrom(start Key, fn func(Scan, Key, Item))

	// Scans all items in the index - even those that are candidates for gc.
	// This is intended to create a 'distributable' convergent index.
	RawScan(fn func(Scan, Key, Item))

	// Scans the index starting with the minimum node.
	RawScanFrom(start Key, fn func(Scan, Key, Item))
}

// Update methods for an index.
type Update interface {
	View

	// Puts the value at the key.
	Put(key Key, val interface{}, ver int) bool

	// Deletes the key. Future reads will return nil for the key.
	Del(key Key, ver int) bool

	// Permanentally deletes the key.  (ie retains no references.)
	DelNow(key Key)
}

// the basic index item.
type item struct {
	val  interface{}
	ver  int
	time time.Time
}

func (i item) Ver() int {
	return i.ver
}

func (i item) Del() bool {
	return i.val == nil
}

func (i item) Val() interface{} {
	return i.val
}

func (i item) Time() time.Time {
	return i.time
}

// index view implementation (just a simple wrapper over index)
// the expectation is that a lock is being held for the lifetime
// of this object.
type view struct {
	raw  RawView
	time time.Time
}

func (v *view) Time() time.Time {
	return v.time
}

func (v *view) RawGet(key Key) (ret Item) {
	if val := v.raw.Get(key); val != nil {
		ret = val.(Item)
	}
	return
}

func (v *view) Get(key Key) (ret Item) {
	if item := v.RawGet(key); item != nil && ! item.Del() {
		ret = item
	}
	return
}

func (v *view) RawScan(fn func(Scan, Key, Item)) {
	v.raw.Scan(func(s Scan, k Key, v interface{}) {
		fn(s, k, v.(Item))
	})
	return
}

func (v *view) RawScanFrom(start Key, fn func(Scan, Key, Item)) {
	v.raw.ScanFrom(start, func(s Scan, k Key, v interface{}) {
		fn(s, k, v.(Item))
	})
	return
}

func (v *view) Scan(fn func(Scan, Key, Item)) {
	v.RawScan(func(s Scan, k Key, i Item) {
		if ! i.Del() {
			fn(s, k, i)
		}
	})
	return
}

func (v *view) ScanFrom(start Key, fn func(Scan, Key, Item)) {
	v.RawScanFrom(start, func(s Scan, k Key, i Item) {
		if ! i.Del() {
			fn(s, k, i)
		}
	})
	return
}

// update implementation (just a simple wrapper over view)
// the expectation is that a write lock is held during the lifetime
// of this object.
type update struct {
	*view
	raw RawUpdate
}

func (u *update) Put(key Key, val interface{}, ver int) bool {
	new := item{val, ver, u.Time()}
	raw := u.raw.Get(key)
	if raw == nil {
		u.raw.Put(key, new)
		return true
	}

	cur := raw.(Item)
	if cur.Ver() < ver {
		u.raw.Put(key, new)
		return true
	}

	return false
}

func (u *update) Del(key Key, ver int) bool {
	new := item{nil, ver, u.Time()}
	raw := u.raw.Get(key)
	if raw == nil {
		u.raw.Put(key, new)
		return true
	}

	cur := raw.(Item)
	if cur.Ver() <= ver {
		u.raw.Put(key, new)
		return true
	}

	return false
}

func (u *update) DelNow(key Key) {
	u.raw.Del(key)
}

// IMPLEMENTATIONS:
type indexer struct {
	ctx    common.Context
	raw    RawIndex
	wait   sync.WaitGroup
	closed chan struct{}
	closer chan struct{}
}

func NewIndexer(ctx common.Context) Index {
	e := &indexer{
		ctx:    ctx,
		raw:    NewBTreeIndex(32),
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1)}

	coll := newCollector(e)
	coll.start()

	return e
}

func (e *indexer) Close() error {
	select {
	case <-e.closed:
		return errors.New("Index already closing")
	case e.closer <- struct{}{}:
	}

	close(e.closed)
	e.wait.Wait()
	return nil
}

func (e *indexer) Size() (ret int) {
	return e.raw.Size()
}

func (e *indexer) Read(fn func(View)) {
	e.raw.Update(func(u RawUpdate) {
		fn(&view{u, time.Now()})
	})
}

func (e *indexer) Update(fn func(Update)) {
	e.raw.Update(func(u RawUpdate) {
		fn(&update{&view{u, time.Now()}, u})
	})
}

// A simple garbage collector
type collector struct {
	indexer *indexer
	logger  common.Logger
	gcExp   time.Duration
	gcPer   time.Duration
}

func newCollector(indexer *indexer) *collector {
	conf := indexer.ctx.Config()
	c := &collector{
		indexer: indexer,
		logger:  indexer.ctx.Logger(),
		gcExp:   conf.OptionalDuration("amoeba.index.gc.expiration", 30*time.Minute),
		gcPer:   conf.OptionalDuration("amoeba.index.gc.cycle", 30*time.Second),
	}

	return c
}

func (c *collector) start() {
	c.indexer.wait.Add(1)
	go c.run()
}

func (d *collector) run() {
	defer d.indexer.wait.Done()
	defer d.logger.Debug("GC shutting down")

	d.logger.Debug("Running GC every [%v] with expiration [%v]", d.gcPer, d.gcExp)

	ticker := time.Tick(d.gcPer)
	for {
		select {
		case <-d.indexer.closed:
			return
		case <-ticker:
			d.runGcCycle(d.gcExp)
		}
	}
}

func (d *collector) runGcCycle(gcExp time.Duration) {
	d.indexer.Update(func(u Update) {
		d.logger.Debug("GC cycle [%v] for items older than [%v]", u.Time(), gcExp)
		deleteDeadKeys(u.(*update).raw, collectDeadKeys(u, u.Time(), gcExp))
	})
}

func deleteDeadKeys(u RawUpdate, keys []Key) {
	for _, key := range keys {
		u.Del(key)
	}
}

func collectDeadKeys(view View, gcStart time.Time, gcDead time.Duration) []Key {
	dead := make([]Key, 0, 128)

	view.RawScan(func(scan Scan, key Key, item Item) {
		if val := item.Val(); val == nil && gcStart.Sub(item.Time()) >= gcDead {
			dead = append(dead, key)
			return
		}
	})

	return dead
}

package amoeba

import (
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
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

// The core storage abstraction.
type Indexer interface {
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

	// Scans the index starting with the minimum node.
	Scan(fn func(*Scan, Key, Item))

	// Scans beginning at the first node that is greater than or
	// equal to the input key.
	ScanFrom(start Key, fn func(*Scan, Key, Item))
}

type Update interface {
	View

	// Puts the value at the key.
	Put(key Key, val Val, ver int) bool

	// Deletes the key. Future reads will return nil for the key.
	Del(key Key, ver int) bool

	// Deletes the key, but doesn't
	DelNow(key Key)
}

// A simple sortable interface
type Sortable interface {
	Compare(Sortable) int
}

// A generic key type.  The only requirement of keys is they sortable
type Key interface {
	Sortable
}

// A generic value type.  This is just the empty interface.
type Val interface{}

// Simple item struct for return values.
type Item interface {
	Val() Val
	Ver() int
}

// A scan gives consumers influence over scanning behavior.
type Scan struct {
	next Key
	stop bool
}

// Communicates to the executing Scan to move to the item
// that is the smallest node greater than or equal to the
// input key.  If no such node exists, then this behaves
// equivalently to when stop is called.
func (s *Scan) Next(i Key) {
	s.next = i
}

// Communicates to the executing Scan to halt and stop
// further execution.  Once the consumer has returned
// from the scan function, the scan will return.
func (s *Scan) Stop() {
	s.stop = true
}

// Utility functions
func Get(idx Indexer, key Key) Item {
	var item Item
	idx.Read(func(v View) {
		item = v.Get(key)
	})
	return item
}

func Put(idx Indexer, key Key, val Val, ver int) bool {
	var ret bool
	idx.Update(func(u Update) {
		ret = u.Put(key, val, ver)
	})
	return ret
}

func Del(idx Indexer, key Key, ver int) bool  {
	var ret bool
	idx.Update(func(u Update) {
		ret = u.Del(key, ver)
	})
	return ret
}

// IMPLEMENTATIONS:

// index view implementation (just a simple wrapper over index)
// the expectation is that a lock is being held for the lifetime
// of this object.
type view struct {
	index *index
	time  time.Time
}

func (v *view) Time() time.Time {
	return v.time
}

func (v *view) Get(key Key) Item {
	return v.index.Get(key)
}

func (v *view) Scan(fn func(*Scan, Key, Item)) {
	v.index.Scan(fn)
}

func (v *view) ScanFrom(start Key, fn func(*Scan, Key, Item)) {
	v.index.ScanFrom(start, fn)
}

// update implementation (just a simple wrapper over view)
// the expectation is that a write lock is held during the lifetime
// of this object.
type update struct {
	view *view
}

func (u *update) Time() time.Time {
	return u.view.Time()
}

func (u *update) Get(key Key) Item {
	return u.view.Get(key)
}

func (u *update) Scan(fn func(*Scan, Key, Item)) {
	u.view.Scan(fn)
}

func (u *update) ScanFrom(start Key, fn func(*Scan, Key, Item)) {
	u.view.ScanFrom(start, fn)
}

func (u *update) Put(key Key, val Val, ver int) bool {
	return u.view.index.Put(key, val, ver, u.Time())
}

func (u *update) Del(key Key, ver int) bool {
	return u.view.index.Del(key, ver, u.Time())
}

func (u *update) DelNow(key Key) {
	u.view.index.DelNow(key)
}

type indexer struct {
	ctx    common.Context
	index  *index
	lock   sync.RWMutex
	wait   sync.WaitGroup
	closed chan struct{}
	closer chan struct{}
}

func NewIndexer(ctx common.Context) Indexer {
	e := &indexer{
		ctx:    ctx,
		index:  newIndex(32),
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
	e.Read(func(v View) {
		ret = e.index.Size()
	})
	return
}

func (e *indexer) Read(fn func(View)) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	fn(&view{time: time.Now(), index: e.index})
}

func (e *indexer) Update(fn func(Update)) {
	e.lock.Lock()
	defer e.lock.Unlock()
	fn(&update{&view{time: time.Now(), index: e.index}})
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
		gcExp:   conf.OptionalDuration("amoeba.index.gc.expiration", 24*60*time.Minute),
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
		d.logger.Debug("GC cycle begin [%v]", u.Time())

		<-concurrent.NewFuture(func() interface{} {
			deleteDeadKeys(d.indexer.index, collectDeadKeys(d.indexer.index, u.Time(), gcExp))
			return nil
		})
	})
}

func deleteDeadKeys(index *index, keys []Key) {
	for _, key := range keys {
		item := indexKey{key}
		delete(index.table, item)
		index.tree.Delete(item)
	}
}

func collectDeadKeys(index *index, gcStart time.Time, gcDead time.Duration) []Key {
	dead := make([]Key, 0, 128)

	index.Scan(func(scan *Scan, key Key, it Item) {
		item := it.(item)

		if val := item.Val(); val == nil && gcStart.Sub(item.Time) >= gcDead {
			dead = append(dead, key)
			return
		}
	})

	return dead
}

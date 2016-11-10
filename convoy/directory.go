package convoy

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/pkopriv2/bourne/btree"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	uuid "github.com/satori/go.uuid"
)

// the primary index data type
type datum struct {
	Time    time.Time
	Deleted bool
	Version int
	Member  Member
}

// the secondary index data type. (just a pointer to primary)
type ref struct {
	Deleted bool
	Version int
	Time    time.Time
}

func (r ref) String() string {
	return fmt.Sprintf("(%v, %v, %v)", r.Deleted, r.Version, r.Time)
}

// Every member contributes a small key-value db to the directory.
type item struct {
	Id  uuid.UUID
	Key string
	Val string
}

func (k item) IncrementId() item {
	return item{incUuid(k.Id), k.Key, k.Val}
}

func (k item) IncrementKey() item {
	return item{k.Id, incString(k.Key), k.Val}
}

func (k item) IncrementVal() item {
	return item{k.Id, k.Key, incString(k.Val)}
}

// An item that is sorted according to the precedence rules: Key/Id/Val
type kiv item

func (k kiv) String() string {
	return fmt.Sprintf("/key:%v/id:%v/val:%v", k.Key, k.Id, k.Val)
}

func (k kiv) IncrementKey() kiv {
	return kiv(item(k).IncrementKey())
}

func (k kiv) IncrementId() kiv {
	return kiv(item(k).IncrementId())
}

func (k kiv) IncrementVal() kiv {
	return kiv(item(k).IncrementVal())
}

func (k kiv) Compare(o kiv) int {
	if ret := bytes.Compare([]byte(k.Key), []byte(o.Key)); ret != 0 {
		return ret
	}

	if ret := bytes.Compare(k.Id.Bytes(), o.Id.Bytes()); ret != 0 {
		return ret
	}

	return bytes.Compare([]byte(k.Val), []byte(o.Val))
}

func (k kiv) Less(other btree.Item) bool {
	return k.Compare(other.(kiv)) < 0
}

// A transactional data view into the directory.   This is extremely
// dangerous and only people who really know what they're doing should
// use this directly.  Instead, use the convenience functions for
// generating common mutations over a transaction
//
// ** INTERNAL ONLY **
type tx struct {
	Time    time.Time
	Primary map[uuid.UUID]datum
	Kiv     *index
}


// Adds a member to the directory
func dirPutMemberTx(id uuid.UUID, mem Member, ver int) func(*tx) error {
	return func(tx *tx) error {
		updated := datum{Member: mem, Version: ver, Time: tx.Time}

		existing, ok := tx.Primary[id]
		if !ok {
			tx.Primary[id] = updated
			return nil
		}

		if existing.Version < ver {
			tx.Primary[id] = updated
			return nil
		}

		return nil
	}
}

// Removes a member from the directory.
func dirDelMemberTx(id uuid.UUID, ver int) func(*tx) error {
	return func(tx *tx) error {
		updated := datum{Deleted: true, Version: ver, Time: tx.Time}

		existing, ok := tx.Primary[id]
		if !ok {
			tx.Primary[id] = updated
			return nil
		}

		if existing.Version <= ver { // notice equality
			tx.Primary[id] = updated
			return nil
		}

		return nil
	}
}

// Puts the key value to the directory for the member at the given id.
func dirPutKeyValueTx(id uuid.UUID, key string, val string, ver int) func(*tx) error {
	return func(tx *tx) error {
		updated := ref{Time: tx.Time, Version: ver}

		var existingRef *ref
		var existingKiv *kiv
		tx.Kiv.ScanAt(kiv{Id: id, Key: key}, func(scan *indexScan, key indexKey) {
			kiv := key.(kiv)
			ref := tx.Kiv.Get(existingKiv).(ref)

			existingKiv = &kiv
			existingRef = &ref
			scan.Stop()
		})

		if existingRef == nil {
			tx.Kiv.Put(kiv{id, key, val}, updated)
			return nil
		}

		if existingRef.Version < ver {
			tx.Kiv.Remove(existingKiv)
			tx.Kiv.Put(kiv{id, key, val}, updated)
			return nil
		}

		return nil
	}
}

// Removes a key value from the directory.
func dirDelKeyValueTx(id uuid.UUID, key string, ver int) func(*tx) error {
	return func(tx *tx) error {
		updated := ref{Deleted: true, Time: tx.Time, Version: ver}

		var existingRef *ref
		var existingKiv *kiv
		tx.Kiv.ScanAt(kiv{Id: id, Key: key}, func(scan *indexScan, key indexKey) {
			kiv := key.(kiv)
			ref := tx.Kiv.Get(existingKiv).(ref)

			existingKiv = &kiv
			existingRef = &ref
			scan.Stop()
		})

		if existingRef == nil {
			tx.Kiv.Put(kiv{Id: id, Key: key}, updated)
			return nil
		}

		if existingRef.Version < ver {
			tx.Kiv.Remove(existingKiv)
			tx.Kiv.Put(kiv{Id: id, Key: key}, updated)
			return nil
		}

		return nil
	}
}

// the core storage type.
type directory struct {
	ctx    common.Context
	lock   sync.RWMutex
	id     map[uuid.UUID]datum
	kiv    *index // index of kiv -> ref
	closed chan struct{}
	closer chan struct{}
	wait   sync.WaitGroup
}

func newDirectory(ctx common.Context) *directory {
	dir := &directory{
		ctx:    ctx,
		kiv:    newIndex(),
		id:     make(map[uuid.UUID]datum),
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1)}

	dir.startGc()
	return dir
}

func (d *directory) read(fn func(*tx) error) error {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return fn(&tx{time.Now(), d.id, d.kiv})
}

func (d *directory) write(fn func(*tx) error) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	return fn(&tx{time.Now(), d.id, d.kiv})
}

func (d *directory) Close() error {
	select {
	case <-d.closed:
		return errors.New("Directory already closing")
	case d.closer <- struct{}{}:
	}

	close(d.closed)
	d.wait.Wait()
	return nil
}

func (d *directory) startGc() {
	collector := newCollector(d)

	d.wait.Add(1)
	go func() {
		defer d.wait.Done()

		select {
		case <-d.closed:
			collector.Close()
		}
	}()
}

type collector struct {
	dir    *directory
	closed chan struct{}
	closer chan struct{}
	wait   sync.WaitGroup
}

func newCollector(dir *directory) *collector {
	c := &collector{
		dir:    dir,
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1),
	}

	c.wait.Add(1)
	go c.run()
	return c
}

func (c *collector) Close() error {
	select {
	case <-c.closed:
		return errors.New("Collector already closing")
	case c.closer <- struct{}{}:
	}

	close(c.closed)
	c.wait.Wait()
	return nil
}

func (d *collector) run() {
	defer d.wait.Done()

	ctx := d.dir.ctx
	conf := ctx.Config()
	logger := ctx.Logger()
	defer logger.Debug("GC shutting down")

	gcExp := conf.OptionalDuration("convoy.directory.gc.data.expiration", 24*60*time.Minute)
	gcPer := conf.OptionalDuration("convoy.directory.gc.cycle.time", 30*time.Second)

	logger.Debug("Running GC every [%v] with expiration [%v]", gcPer, gcExp)

	// TODO: Run benchmarks to determine if we should do a stop the world collection,
	// or if we should do more of a concurrent-mark and sweep approach.
	ticker := time.Tick(gcPer)
	for {
		var gcBeg time.Time
		select {
		case <-d.closed:
			return
		case gcBeg = <-ticker:
		}

		// clean it up
		d.runGcCycle(gcBeg, gcExp)
	}
}

func (d *collector) runGcCycle(gcBeg time.Time, gcExp time.Duration) {
	logger := d.dir.ctx.Logger()

	d.dir.write(func(tx *tx) error {
		logger.Debug("Directory GC Cycle Start [%v]", gcBeg)

		// collect all dead data.
		datums := concurrent.NewFuture(func() interface{} {
			return collectDeadDatums(tx, gcBeg, gcExp)
		})

		// collect all dead kiv ref's
		refs := concurrent.NewFuture(func() interface{} {
			return collectDeadKivRefs(tx, gcBeg, gcExp)
		})

		// delete data
		deleteKivRefs(tx, (<-refs).([]kiv))
		deleteDatums(tx, (<-datums).([]uuid.UUID))
		return nil
	})
}

func collectDeadDatums(tx *tx, gcStart time.Time, gcDead time.Duration) []uuid.UUID {
	dead := make([]uuid.UUID, 0, 128)
	for id, datum := range tx.Primary {
		if datum.Deleted && gcStart.Sub(datum.Time) >= gcDead {
			dead = append(dead, id)
		}
	}

	return dead
}

func collectDeadKivRefs(tx *tx, gcStart time.Time, gcDead time.Duration) []kiv {
	dead := make([]kiv, 0, 128)

	tx.Kiv.Scan(func(scan *indexScan, key indexKey) {
		kiv := key.(kiv)
		ref := tx.Kiv.Get(key).(ref)
		if ref.Deleted && gcStart.Sub(ref.Time) >= gcDead {
			dead = append(dead, kiv)
			return
		}

		if _, ok := tx.Primary[kiv.Id]; !ok {
			dead = append(dead, kiv)
			return
		}
	})

	return dead
}

// assumes a write lock is taken
func deleteKivRefs(tx *tx, refs []kiv) {
	for _, kiv := range refs {
		tx.Kiv.Remove(kiv)
	}
}

// assumes a write lock is taken
func deleteDatums(tx *tx, ids []uuid.UUID) {
	// make lookup table
	lookup := make(map[uuid.UUID]bool)
	for _, id := range ids {
		lookup[id] = true
	}

	// lookup any refs that would be orphaned by delete.
	deadRefs := make([]kiv, 0, 128)
	tx.Kiv.Scan(func(scan *indexScan, key indexKey) {
		kiv := key.(kiv)
		if lookup[kiv.Id] {
			deadRefs = append(deadRefs, kiv)
		}
	})

	// delete the refs
	for _, key := range deadRefs {
		tx.Kiv.Remove(key)
	}

	// delete the datums
	for _, id := range ids {
		delete(tx.Primary, id)
	}
}

// a few helper methods
func incBytes(val []byte) []byte {
	cur := new(big.Int)
	cop := make([]byte, len(val))
	copy(cop, val)

	cur.SetBytes(val)
	cur.Add(cur, big.NewInt(1))
	return cur.Bytes()
}

func incUuid(id uuid.UUID) uuid.UUID {
	inc := incBytes(id.Bytes())
	buf := make([]byte, 16)
	for i, b := range inc {
		buf[i] = b
	}

	id, err := uuid.FromBytes(buf)
	if err != nil {
		panic(err)
	}

	return id
}

func incString(val string) string {
	return string(incBytes([]byte(val)))
}

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
	"github.com/pkopriv2/bourne/enc"
	uuid "github.com/satori/go.uuid"
)

// The directory is the core storage engine of the convoy replicas.
// It's primary purpose is to maintain 1.) the listing of members
// and 2.) allow searchable access to the members' datastores.

// the secondary index data type. (just a pointer to primary)
type datum struct {
	Deleted bool
	Version int
	Time    time.Time
}

func (r datum) String() string {
	return fmt.Sprintf("(%v, %v, %v)", r.Deleted, r.Version, r.Time)
}

// Events encapsulate a specific mutation, allowing them to
// be stored or replicated for future use.  Events are stored
// in time sorted order - but are NOT made durable.  Therefore,
// this log is NOT suitable for disaster recovery.  The current
// approach is to recreate the log from a "live" member.
type event interface {
	enc.Writable

	Apply(*db)
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

// the ikv key index (Id-Key-Val)
type ikv item

func (k ikv) String() string {
	return fmt.Sprintf("/id:%v/key:%s/val:%v", k.Key, k.Id, k.Val)
}

func (k ikv) IncrementKey() ikv {
	return ikv(item(k).IncrementKey())
}

func (k ikv) IncrementId() ikv {
	return ikv(item(k).IncrementId())
}

func (k ikv) IncrementVal() ikv {
	return ikv(item(k).IncrementVal())
}

func (k ikv) Compare(o ikv) int {
	if ret := bytes.Compare(k.Id.Bytes(), o.Id.Bytes()); ret != 0 {
		return ret
	}

	if ret := bytes.Compare([]byte(k.Key), []byte(o.Key)); ret != 0 {
		return ret
	}

	return bytes.Compare([]byte(k.Val), []byte(o.Val))
}

func (k ikv) Less(other btree.Item) bool {
	return k.Compare(other.(ikv)) < 0
}

// The kiv key index (Key-Id-Val)
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

// index of time to event
type te uint64

func (t te) Less(than btree.Item) bool {
	return t < than.(te)
}

// A transactional data view into the directory.   Whiles this does guarantee
// a consistent view, updates to the data are visible immediately.  There is
// no rollback mechanism.  This is extremely dangerous and only people who really
// know what they're doing should use this directly.  Instead, use the convenience
// functions for generating common mutations over a transaction
//
// ** INTERNAL ONLY **
type db struct {
	Clock uint64
	Time  time.Time
	Te    *index // time -> event index
	Ikv   *index
	Kiv   *index
}

// Puts the key value to the directory for the member at the given id.
func dirIndexKiv(db *db, key kiv, ver int, del bool) {
	updated := datum{Time: db.Time, Version: ver, Deleted: del}

	var existingKiv *kiv
	var existingDatum *datum
	db.Kiv.ScanAt(kiv{Key: key.Key, Id: key.Id}, func(scan *indexScan, key indexKey) {
		kiv := key.(kiv)
		datum := db.Kiv.Get(kiv).(datum)

		existingKiv = &kiv
		existingDatum = &datum
		scan.Stop()
	})

	if existingDatum == nil {
		db.Kiv.Put(key, updated)
		return
	}

	if existingDatum.Version < ver {
		db.Kiv.Remove(existingKiv)
		db.Kiv.Put(key, updated)
		return
	}
}

// Puts the key value to the directory for the member at the given id.
func dirIndexIkv(db *db, key ikv, ver int, del bool) {
	updated := datum{Time: db.Time, Version: ver, Deleted: del}

	var existingIkv *ikv
	var existingDatum *datum
	db.Ikv.ScanAt(ikv{Key: key.Key, Id: key.Id}, func(scan *indexScan, key indexKey) {
		ikv := key.(ikv)
		datum := db.Ikv.Get(ikv).(datum)

		existingIkv = &ikv
		existingDatum = &datum
		scan.Stop()
	})

	if existingDatum == nil {
		db.Ikv.Put(key, updated)
		return
	}

	if existingDatum.Version < ver {
		db.Ikv.Remove(existingIkv)
		db.Ikv.Put(key, updated)
		return
	}
}

// Puts the key value to the directory for the member at the given id.
func dirIndexEvent(db *db, key te, event event) {
	db.Ikv.Put(key, event)
}

// The primary data event type.
type dataEvent struct {
	Id  uuid.UUID
	Key string
	Val string
	Ver int
	Del bool
}

func newAddDatum(id uuid.UUID, key string, val string, ver int) *dataEvent {
	return &dataEvent{id, key, val, ver, false}
}

func newDelDatum(id uuid.UUID, key string, ver int) *dataEvent {
	return &dataEvent{id, key, "", ver, true}
}

func readDataEvent(r enc.Reader) (*dataEvent, error) {
	id, err := readUUID(r, "id")
	if err != nil {
		return nil, err
	}

	event := &dataEvent{Id: id}
	if err := r.Read("key", &event.Key); err != nil {
		return nil, err
	}
	if err := r.Read("val", &event.Val); err != nil {
		return nil, err
	}
	if err := r.Read("ver", &event.Ver); err != nil {
		return nil, err
	}
	if err := r.Read("del", &event.Del); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *dataEvent) Write(w enc.Writer) {
	w.Write("id", e.Id.String())
	w.Write("key", e.Key)
	w.Write("val", e.Val)
	w.Write("ver", e.Ver)
	w.Write("del", e.Del)
}

func (e *dataEvent) Apply(db *db) {
	item := item{e.Id, e.Key, e.Val}

	dirIndexEvent(db, te(db.Clock), e)
	dirIndexKiv(db, kiv(item), e.Ver, e.Del)
	dirIndexIkv(db, ikv(item), e.Ver, e.Del)
}

// the core storage type.
type directory struct {
	ctx    common.Context
	lock   sync.RWMutex
	clock  uint64
	kiv    *index // index of kiv -> datum
	ikv    *index // index of ikv -> datum
	te     *index // index of te  -> event
	closed chan struct{}
	closer chan struct{}
	wait   sync.WaitGroup
}

func newDirectory(ctx common.Context) *directory {
	dir := &directory{
		ctx:    ctx,
		kiv:    newIndex(),
		ikv:    newIndex(),
		te:     newIndex(),
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1)}

	dir.startGc()
	return dir
}

func (d *directory) read(fn func(*db)) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	fn(&db{d.clock, time.Now(), d.te, d.ikv, d.kiv})
}

func (d *directory) write(fn func(*db)) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.clock++
	fn(&db{d.clock, time.Now(), d.te, d.ikv, d.kiv})
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

// The directory collector.  Keeps it clean!
type collector struct {
	dir    *directory
	gcExp  time.Duration
	gcPer  time.Duration
	closed chan struct{}
	closer chan struct{}
	wait   sync.WaitGroup
}

func newCollector(dir *directory) *collector {
	conf := dir.ctx.Config()

	c := &collector{
		dir:    dir,
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1),
		gcExp:  conf.OptionalDuration("convoy.directory.gc.data.expiration", 24*60*time.Minute),
		gcPer:  conf.OptionalDuration("convoy.directory.gc.cycle.time", 30*time.Second),
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
	logger := d.dir.ctx.Logger()
	defer d.wait.Done()
	defer logger.Debug("GC shutting down")

	logger.Debug("Running GC every [%v] with expiration [%v]", d.gcPer, d.gcExp)

	ticker := time.Tick(d.gcPer)
	for {
		select {
		case <-d.closed:
			return
		case <-ticker:
		}

		// clean it up
		d.runGcCycle(d.gcExp)
	}
}

func (d *collector) runGcCycle(gcExp time.Duration) {
	d.dir.write(func(db *db) {
		d.dir.ctx.Logger().Debug("Gc begin [%v]", db.Time)

		ikvs := concurrent.NewFuture(func() interface{} {
			deleteDeadDatums(db.Ikv, collectDeadKeys(db.Ikv, db.Time, gcExp))
			return nil
		})

		kivs := concurrent.NewFuture(func() interface{} {
			deleteDeadDatums(db.Kiv, collectDeadKeys(db.Kiv, db.Time, gcExp))
			return nil
		})

		<-ikvs
		<-kivs
	})
}

func deleteDeadDatums(idx *index, keys []indexKey) {
	for _, key := range keys {
		idx.Remove(key)
	}
}

func collectDeadKeys(idx *index, gcStart time.Time, gcDead time.Duration) []indexKey {
	dead := make([]indexKey, 0, 128)

	idx.Scan(func(scan *indexScan, key indexKey) {
		datum := idx.Get(key).(datum)
		if datum.Deleted && gcStart.Sub(datum.Time) >= gcDead {
			dead = append(dead, key)
			return
		}
	})

	return dead
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

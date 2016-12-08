package convoy

import (
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

// System reserved keys.  Consumers should consider the: /Convoy/
const (
	memberEnabledAttr = "/Convoy/Member/Enabled"
	memberHostAttr    = "/Convoy/Member/Host"
	memberPortAttr    = "/Convoy/Member/Port"
	memberStatusAttr  = "/Convoy/Member/Status"
)

// Core storage abstractions.

type item struct {
	MemId  uuid.UUID
	MemVer int

	Attr    string
	Val string
	Ver int
	Del bool

	Time time.Time
}

// member status
type status struct {
	Version int
	Enabled bool
	Since   time.Time
}

// the core storage type
type storage struct {
	Context common.Context
	Logger  common.Logger
	Data    amoeba.RawIndex
	Roster  map[uuid.UUID]status // sync'ed with data lock
	Wait    sync.WaitGroup
	Closed  chan struct{}
	Closer  chan struct{}
}

func newStorage(ctx common.Context, logger common.Logger) *storage {
	s := &storage{
		Context: ctx,
		Logger: logger.Fmt("Storage"),
		Data:   amoeba.NewBTreeIndex(32),
		Roster: make(map[uuid.UUID]status),
		Closed: make(chan struct{}),
		Closer: make(chan struct{}, 1)}

	coll := newStorageGc(s)
	coll.start()

	return s
}

func (s *storage) Close() (err error) {
	select {
	case <-s.Closed:
		return errors.New("Index already closing")
	case s.Closer <- struct{}{}:
	}

	close(s.Closed)
	s.Wait.Wait()
	return nil
}

func (d *storage) Status() (ret map[uuid.UUID]status) {
	ret = make(map[uuid.UUID]status)
	d.Data.Read(func(data amoeba.RawView) {
		for k, v := range d.Roster {
			ret[k] = v
		}
	})
	return
}

func (d *storage) View(fn func(*view)) {
	d.Data.Read(func(data amoeba.RawView) {
		fn(&view{data, d.Roster, time.Now()})
	})
}

func (d *storage) Update(fn func(*update)) (ret []item) {
	d.Data.Update(func(data amoeba.RawUpdate) {
		update := &update{&view{data, d.Roster, time.Now()}, data, d.Roster, make([]item, 0, 8)}
		defer func() {
			ret = update.Items
		}()

		fn(update)
	})
	return
}

func (d *storage) All() (ret []item) {
	ret = make([]item, 0, d.Data.Size())
	d.View(func(v *view) {
		v.ScanAll(func(s amoeba.Scan, i item) {
			ret = append(ret, i)
		})
	})
	return
}

// Transactional view
type view struct {
	Raw    amoeba.RawView
	Roster map[uuid.UUID]status
	Now    time.Time
}

func (u *view) Time() time.Time {
	return u.Now
}

func (u *view) Status(id uuid.UUID) (int, bool) {
	status, ok := u.Roster[id]
	if !ok {
		return 0, false
	}

	return status.Version, status.Enabled
}

func (u *view) Get(id uuid.UUID, attr string) (ret item, found bool) {
	memberVer, memberEnabled := u.Status(id)
	if !memberEnabled {
		return
	}

	rawVal, rawFound := storageGet(u.Raw, id, attr)
	if !rawFound || rawVal.Del {
		return
	}

	if memberVer != rawVal.MemVer {
		return
	}

	return item{id, rawVal.MemVer, attr, rawVal.Val, rawVal.Ver, false, rawVal.Time}, true
}

func (u *view) ScanAll(fn func(amoeba.Scan, item)) {
	storageScan(u.Raw, func(s amoeba.Scan, k storageKey, v storageValue) {
		fn(s, item{k.Id, v.MemVer, k.Attr, v.Val, v.Ver, v.Del, v.Time})
	})
}

func (u *view) ScanLive(fn func(amoeba.Scan, item)) {
	u.ScanAll(func(s amoeba.Scan, i item) {
		if !i.Del {
			fn(s, i)
		}
	})
}

// Transactional update
type update struct {
	*view
	Raw    amoeba.RawUpdate
	Roster map[uuid.UUID]status
	Items  []item
}

func (u *update) Put(memId uuid.UUID, memVer int, attr string, attrVal string, attrVer int) (ret bool) {
	ret = storagePut(u.Raw, u.Now, memId, memVer, attr, attrVal, attrVer)
	if !ret {
		return
	}

	u.Items = append(u.Items, item{memId, memVer, attr, attrVal, attrVer, false, u.Now})
	if attr == memberEnabledAttr {
		u.Roster[memId] = status{memVer, attrVal == "true", u.Now}
	}

	return
}

func (u *update) Del(memId uuid.UUID, memVer int, attr string, attrVer int) (ret bool) {
	ret = storageDel(u.Raw, u.Now, memId, memVer, attr, attrVer)
	if !ret {
		return
	}

	u.Items = append(u.Items, item{memId, memVer, attr, "", attrVer, true, u.Now})
	if attr == memberEnabledAttr {
		u.Roster[memId] = status{memVer, false, u.Now}
	}
	return
}

func (u *update) Join(id uuid.UUID, ver int) bool {
	return u.Put(id, ver, memberEnabledAttr, "true", 0)
}

func (u *update) Leave(id uuid.UUID, ver int) bool {
	return u.Del(id, ver, memberEnabledAttr, 0)
}

// the amoeba key type
type storageKey struct {
	Attr string
	Id   uuid.UUID
}

func (k storageKey) String() string {
	return fmt.Sprintf("/attr:%v/id:%v", k.Attr, k.Id)
}

func (k storageKey) Compare(other amoeba.Sortable) int {
	o := other.(storageKey)
	if ret := amoeba.CompareStrings(k.Attr, o.Attr); ret != 0 {
		return ret
	}
	return amoeba.CompareUUIDs(k.Id, o.Id)
}

// the amoeba value type
type storageValue struct {
	MemVer  int
	Ver int
	Val string
	Del bool

	Time time.Time
}

// low level data manipulation functions
func storageGet(data amoeba.RawView, id uuid.UUID, attr string) (ret storageValue, found bool) {
	raw := data.Get(storageKey{attr, id})
	if raw == nil {
		return
	}

	return raw.(storageValue), true
}

func storagePut(data amoeba.RawUpdate, time time.Time, memId uuid.UUID, memVer int, attr string, attrVal string, attrVer int) bool {
	if cur, found := storageGet(data, memId, attr); found {
		if memVer < cur.MemVer {
			return false
		}

		if memVer == cur.MemVer && attrVer <= cur.Ver {
			return false
		}
	}

	data.Put(storageKey{attr, memId}, storageValue{memVer, attrVer, attrVal, true, time})
	return true
}

func storageDel(data amoeba.RawUpdate, time time.Time, memId uuid.UUID, memVer int, attr string, attrVer int) bool {
	if cur, found := storageGet(data, memId, attr); found {
		if memVer < cur.MemVer {
			return false
		}

		if memVer == cur.MemVer && attrVer < cur.Ver {
			return false
		}
	}

	data.Put(storageKey{attr, memId}, storageValue{memVer, attrVer, "", true, time})
	return true
}

func storageScan(data amoeba.RawView, fn func(amoeba.Scan, storageKey, storageValue)) {
	data.Scan(func(s amoeba.Scan, k amoeba.Key, i interface{}) {
		key := k.(storageKey)
		val := i.(storageValue)
		fn(s, key, val)
	})
}

// A simple garbage storeGc
type storageGc struct {
	store  *storage
	logger common.Logger
	gcExp  time.Duration
	gcPer  time.Duration
}

func newStorageGc(store *storage) *storageGc {
	conf := store.Context.Config()
	c := &storageGc{
		store:  store,
		logger: store.Logger.Fmt("Gc"),
		gcExp:  conf.OptionalDuration("convoy.index.gc.expiration", 30*time.Minute),
		gcPer:  conf.OptionalDuration("convoy.index.gc.cycle", time.Minute),
	}

	return c
}

func (c *storageGc) start() {
	// c.store.Wait.Add(1)
	// go c.run()
}

func (d *storageGc) run() {
	defer d.store.Wait.Done()
	defer d.logger.Debug("GC shutting down")

	d.logger.Debug("Running GC every [%v] with expiration [%v]", d.gcPer, d.gcExp)

	ticker := time.Tick(d.gcPer)
	for {
		select {
		case <-d.store.Closed:
			return
		case <-ticker:
			d.runGcCycle(d.gcExp)
		}
	}
}

func (d *storageGc) runGcCycle(gcExp time.Duration) {
	d.store.Update(func(u *update) {
		d.logger.Debug("GC cycle [%v] for items older than [%v]", u.Time(), gcExp)
		deleteDeadItems(u.Raw, collectMemberItems(u.view, collectDeadMembers(u.Roster, u.Time(), gcExp)))
		deleteDeadItems(u.Raw, collectDeadItems(u.view, u.Time(), gcExp))
	})
}

func deleteDeadItems(u amoeba.RawUpdate, items []item) {
	for _, i := range items {
		u.Del(storageKey{i.Attr, i.MemId})
	}
}

func collectMemberItems(v *view, dead map[uuid.UUID]struct{}) []item {
	ret := make([]item, 0, 128)
	v.ScanAll(func(s amoeba.Scan, i item) {
		if _, ok := dead[i.MemId]; ok {
			ret = append(ret, i)
		}
	})
	return ret
}

func collectDeadMembers(roster map[uuid.UUID]status, gcStart time.Time, gcDead time.Duration) map[uuid.UUID]struct{} {
	dead := make(map[uuid.UUID]struct{})
	for id, status := range roster {
		if !status.Enabled && gcStart.Sub(status.Since) > gcDead {
			dead[id] = struct{}{}
		}
	}
	return dead
}

func collectDeadItems(v *view, gcStart time.Time, gcDead time.Duration) []item {
	dead := make([]item, 0, 128)
	v.ScanAll(func(s amoeba.Scan, i item) {
		if !i.Del && gcStart.Sub(i.Time) > gcDead {
			dead = append(dead, i)
		}
	})

	return dead
}

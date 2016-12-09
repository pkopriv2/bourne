package convoy

import (
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// System reserved keys.  Consumers should consider the: /Convoy/ namespace off limits!
const (
	memberMembershipAttr = "Convoy.Member.Joined"
	memberHealthAttr     = "Convoy.Member.Status"
	memberHostAttr       = "Convoy.Member.Host"
	memberPortAttr       = "Convoy.Member.Port"
)

// Core storage abstractions.

type item struct {
	MemId  uuid.UUID
	MemVer int

	Attr string
	Val  string
	Ver  int
	Del  bool

	// Internal only.
	Time time.Time
}

func readItem(r scribe.Reader) (item, error) {
	item := &item{}

	id, err := scribe.ReadUUID(r, "memId")
	if err != nil {
		return *item, err
	}

	item.MemId = id

	if err := r.Read("attr", &item.Attr); err != nil {
		return *item, err
	}
	if err := r.Read("val", &item.Val); err != nil {
		return *item, err
	}
	if err := r.Read("ver", &item.Ver); err != nil {
		return *item, err
	}
	if err := r.Read("del", &item.Del); err != nil {
		return *item, err
	}

	return *item, nil
}

func (i item) Write(w scribe.Writer) {
	w.Write("memId", i.MemId.String())
	w.Write("memVer", i.MemVer)
	w.Write("attr", i.Attr)
	w.Write("val", i.Val)
	w.Write("ver", i.Ver)
	w.Write("del", i.Del)
}

func (i item) Apply(u *update) bool {
	if i.Del {
		return u.Del(i.MemId, i.MemVer, i.Attr, i.Ver)
	} else {
		return u.Put(i.MemId, i.MemVer, i.Attr, i.Val, i.Ver)
	}
}

// membership status
type membership struct {
	Version int
	Active  bool
	Since   time.Time
}

func (s membership) String() string {
	var str string
	if s.Active {
		str = "Active"
	} else {
		str = "Inactive" // really means gone
	}

	return fmt.Sprintf("%v(%v)", str, s.Version)
}

type health struct {
	Version int
	Healthy bool
	Since   time.Time
}

func (h health) String() string {
	var str string
	if h.Healthy {
		str = "Healthy"
	} else {
		str = "Unhealthy"
	}

	return fmt.Sprintf("%v(%v) since %v", str, h.Version, h.Since)
}

// the core storage type
type storage struct {
	Context common.Context
	Logger  common.Logger
	Data    amoeba.Index
	roster  map[uuid.UUID]membership // sync'ed with data lock
	health  map[uuid.UUID]health     // sync'ed with data lock
	Wait    sync.WaitGroup
	Closed  chan struct{}
	Closer  chan struct{}
}

func newStorage(ctx common.Context, logger common.Logger) *storage {
	s := &storage{
		Context: ctx,
		Logger:  logger.Fmt("Storage"),
		Data:    amoeba.NewBTreeIndex(32),
		roster:  make(map[uuid.UUID]membership),
		health:  make(map[uuid.UUID]health),
		Closed:  make(chan struct{}),
		Closer:  make(chan struct{}, 1)}

	coll := newStorageGc(s)
	coll.start()

	return s
}

func (s *storage) Close() (err error) {
	select {
	case <-s.Closed:
		return errors.New("Storage already closing")
	case s.Closer <- struct{}{}:
	}

	close(s.Closed)
	s.Wait.Wait()
	return nil
}

func (d *storage) Roster() (ret map[uuid.UUID]membership) {
	ret = make(map[uuid.UUID]membership)
	d.Data.Read(func(data amoeba.View) {
		for k, v := range d.roster {
			ret[k] = v
		}
	})
	return
}

func (d *storage) Health() (ret map[uuid.UUID]health) {
	ret = make(map[uuid.UUID]health)
	d.Data.Read(func(data amoeba.View) {
		for k, v := range d.health {
			ret[k] = v
		}
	})
	return
}

func (d *storage) View(fn func(*view)) {
	d.Data.Read(func(data amoeba.View) {
		fn(&view{data, d.roster, d.health, time.Now()})
	})
}

func (d *storage) Update(fn func(*update)) (ret []item) {
	d.Data.Update(func(data amoeba.Update) {
		update := &update{&view{data, d.roster, d.health, time.Now()}, data, make([]item, 0, 8)}
		defer func() {
			ret = update.items
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
	raw    amoeba.View
	Roster map[uuid.UUID]membership
	Health map[uuid.UUID]health
	now    time.Time
}

func (u *view) Time() time.Time {
	return u.now
}

func (u *view) GetLatest(id uuid.UUID, attr string) (ret item, found bool) {
	status, ok := u.Roster[id]
	if !ok {
		return
	}

	rawVal, rawFound := storageGet(u.raw, id, status.Version, attr)
	if !rawFound {
		return
	}

	return item{id, status.Version, attr, rawVal.Val, rawVal.Ver, false, rawVal.Time}, true
}

func (u *view) GetActive(id uuid.UUID, attr string) (ret item, found bool) {
	status, ok := u.Roster[id]
	if !ok || !status.Active {
		return
	}

	rawVal, rawFound := storageGet(u.raw, id, status.Version, attr)
	if !rawFound || rawVal.Del {
		return
	}

	return item{id, status.Version, attr, rawVal.Val, rawVal.Ver, false, rawVal.Time}, true
}

func (u *view) ScanAll(fn func(amoeba.Scan, item)) {
	storageScan(u.raw, func(s amoeba.Scan, k storageKey, v storageValue) {
		fn(s, item{k.MemId, k.MemVer, k.Attr, v.Val, v.Ver, v.Del, v.Time})
	})
}

func (u *view) ScanActive(fn func(amoeba.Scan, item)) {
	u.ScanAll(func(s amoeba.Scan, i item) {
		if i.Del {
			return
		}

		status, found := u.Roster[i.MemId]
		if !found {
			return
		}

		if status.Version != i.MemVer {
			return
		}

		fn(s, i)
	})
}

// Transactional update
type update struct {
	*view
	raw   amoeba.Update
	items []item
}

func (u *update) Put(memId uuid.UUID, memVer int, attr string, attrVal string, attrVer int) bool {
	ok := storagePut(u.raw, u.now, memId, memVer, attr, attrVal, attrVer)
	if !ok {
		return false
	}

	u.items = append(u.items, item{memId, memVer, attr, attrVal, attrVer, false, u.now})
	switch attr {
	default:
		return true
	case memberMembershipAttr:
		stat, ok := u.Roster[memId]
		if ok {
			if stat.Version >= attrVer {
				return false
			}
		}

		u.Roster[memId] = membership{memVer, true, u.now}
		return true
	case memberHealthAttr:
		cur, ok := u.Health[memId]
		if ok {
			if cur.Version >= attrVer {
				return false
			}
		}

		u.Health[memId] = health{memVer, true, u.now}
		return true
	}
}

func (u *update) Del(memId uuid.UUID, memVer int, attr string, attrVer int) bool {
	ok := storageDel(u.raw, u.now, memId, memVer, attr, attrVer)
	if !ok {
		return false
	}

	u.items = append(u.items, item{memId, memVer, attr, "", attrVer, true, u.now})
	switch attr {
	default:
		return true
	case memberMembershipAttr:
		cur, ok := u.Roster[memId]
		if ok {
			if cur.Version > attrVer {
				return false
			}
		}

		u.Roster[memId] = membership{memVer, false, u.now}
		return true
	case memberHealthAttr:
		cur, ok := u.Health[memId]
		if ok {
			if cur.Version > attrVer {
				return false
			}
		}

		u.Health[memId] = health{memVer, false, u.now}
		return true
	}
}

func (u *update) Join(id uuid.UUID, ver int) bool {
	if !u.Put(id, ver, memberMembershipAttr, "true", 0) {
		return false
	}

	// This shouldn't be able to return false...
	return u.Put(id, ver, memberHealthAttr, "true", 0)
}

func (u *update) Evict(id uuid.UUID, ver int) bool {
	if ! u.Del(id, ver, memberMembershipAttr, 0) {
		return false
	}

	return u.Del(id, ver, memberHealthAttr, 0)
}

func (u *update) Fail(id uuid.UUID, ver int) bool {
	return u.Del(id, ver, memberHealthAttr, 0)
}

// the amoeba key type
type storageKey struct {
	Attr   string
	MemId  uuid.UUID
	MemVer int
}

func (k storageKey) String() string {
	return fmt.Sprintf("/attr:%v/id:%v/ver:%v", k.Attr, k.MemId, k.MemVer)
}

func (k storageKey) Compare(other amoeba.Key) int {
	o := other.(storageKey)
	if ret := amoeba.CompareStrings(k.Attr, o.Attr); ret != 0 {
		return ret
	}

	if ret := amoeba.CompareUUIDs(k.MemId, o.MemId); ret != 0 {
		return ret
	}

	return k.MemVer - o.MemVer
}

// the amoeba value type
type storageValue struct {
	Ver int
	Val string
	Del bool

	Time time.Time
}

// low level data manipulation functions.  These only enforce low-level versioning requirements.
func storageGet(data amoeba.View, memId uuid.UUID, memVer int, attr string) (ret storageValue, found bool) {
	raw := data.Get(storageKey{attr, memId, memVer})
	if raw == nil {
		return
	}

	return raw.(storageValue), true
}

func storagePut(data amoeba.Update, time time.Time, memId uuid.UUID, memVer int, attr string, attrVal string, attrVer int) bool {
	if cur, found := storageGet(data, memId, memVer, attr); found {
		if attrVer <= cur.Ver {
			return false
		}
	}

	data.Put(storageKey{attr, memId, memVer}, storageValue{attrVer, attrVal, false, time})
	return true
}

func storageDel(data amoeba.Update, time time.Time, memId uuid.UUID, memVer int, attr string, attrVer int) bool {
	if cur, found := storageGet(data, memId, memVer, attr); found {
		if attrVer < cur.Ver {
			return false
		}

		if cur.Del {
			return false
		}
	}

	data.Put(storageKey{attr, memId, memVer}, storageValue{attrVer, "", true, time})
	return true
}

func storageScan(data amoeba.View, fn func(amoeba.Scan, storageKey, storageValue)) {
	data.Scan(func(s amoeba.Scan, k amoeba.Key, i interface{}) {
		key := k.(storageKey)
		val := i.(storageValue)
		fn(s, key, val)
	})
}

// A simple garbage collector for the storage api
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
	c.store.Wait.Add(1)
	go c.run()
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
		deleteDeadItems(u.raw, collectMemberItems(u.view, collectDeadMembers(u.Roster, u.Time(), gcExp)))
		deleteDeadItems(u.raw, collectDeadItems(u.view, u.Time(), gcExp))
	})
}

func deleteDeadItems(u amoeba.Update, items []item) {
	for _, i := range items {
		u.Del(storageKey{i.Attr, i.MemId, i.MemVer})
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

func collectDeadMembers(roster map[uuid.UUID]membership, gcStart time.Time, gcDead time.Duration) map[uuid.UUID]struct{} {
	dead := make(map[uuid.UUID]struct{})
	for id, status := range roster {
		if !status.Active && gcStart.Sub(status.Since) > gcDead {
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
			return
		}

		stat, ok := v.Roster[i.MemId]
		if !ok {
			return // roster status hasn't shown up yet...leave it alone for now
		}

		// old, invisible data.
		if i.Ver < stat.Version {
			dead = append(dead, i)
			return
		}
	})

	return dead
}

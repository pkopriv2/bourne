package convoy

import (
	"fmt"
	"sync"
	"time"

	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// System reserved keys.  Consumers should consider the: /Convoy/ namespace off limits!
const (
	memberMembershipAttr = "Convoy.Member.Joined"
	memberHealthAttr     = "Convoy.Member.Health"
	memberHostAttr       = "Convoy.Member.Host"
	memberPortAttr       = "Convoy.Member.Port"
)

// Core storage abstractions.
type item struct {
	MemId  uuid.UUID
	MemVer int

	Key string
	Val string
	Ver int
	Del bool

	// Internal only.
	Time time.Time
}

func readItem(r scribe.Reader) (item, error) {
	item := &item{}

	if err := r.ReadUUID("memId", &item.MemId); err != nil {
		return *item, err
	}
	if err := r.ReadInt("memVer", &item.MemVer); err != nil {
		return *item, err
	}
	if err := r.ReadString("key", &item.Key); err != nil {
		return *item, err
	}
	if err := r.ReadString("val", &item.Val); err != nil {
		return *item, err
	}
	if err := r.ReadInt("ver", &item.Ver); err != nil {
		return *item, err
	}
	if err := r.ReadBool("del", &item.Del); err != nil {
		return *item, err
	}

	return *item, nil
}

func (i item) Write(w scribe.Writer) {
	w.WriteUUID("memId", i.MemId)
	w.WriteInt("memVer", i.MemVer)
	w.WriteString("key", i.Key)
	w.WriteString("val", i.Val)
	w.WriteInt("ver", i.Ver)
	w.WriteBool("del", i.Del)
}

func (i item) Apply(u *update) bool {
	if i.Del {
		return u.Del(i.MemId, i.MemVer, i.Key, i.Ver)
	} else {
		return u.Put(i.MemId, i.MemVer, i.Key, i.Val, i.Ver)
	}
}

func (i item) String() string {
	if i.Del {
		return fmt.Sprintf("Del(%v, %v)", storageKey{i.Key, i.MemId, i.MemVer}, i.Ver)
	} else {
		return fmt.Sprintf("Put(%v, %v): %v", storageKey{i.Key, i.MemId, i.MemVer}, i.Ver, i.Val)
	}
}

// membership status
type membership struct {
	Id      uuid.UUID
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
	Id      uuid.UUID
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

func streamMemberships(ch <-chan []item) <-chan membership {
	ret := make(chan membership)
	go func() {
		for batch := range ch {
			for _, i := range batch {
				if i.Key == memberMembershipAttr {
					ret <- membership{i.MemId, i.MemVer, !i.Del, i.Time}
				}
			}
		}

		close(ret)
	}()
	return ret
}

func streamHealth(ch <-chan []item) <-chan health {
	ret := make(chan health)
	go func() {
		for batch := range ch {
			for _, i := range batch {
				if i.Key == memberHealthAttr {
					ret <- health{i.MemId, i.MemVer, !i.Del, i.Time}
				}
			}
		}

		close(ret)
	}()
	return ret
}

// the core storage type
type storage struct {
	ctx    common.Context
	logger common.Logger

	// All mutable fields sync'ed on datItems
	// Data objects.
	items  amoeba.Index
	roster map[uuid.UUID]membership
	health map[uuid.UUID]health

	// subscriptions
	listeners concurrent.List

	// Utility
	wait   sync.WaitGroup
	closed chan struct{}
	closer chan struct{}
}

func newStorage(ctx common.Context, logger common.Logger) *storage {
	s := &storage{
		ctx:       ctx,
		logger:    logger.Fmt("Storage"),
		items:     amoeba.NewBTreeIndex(32),
		roster:    make(map[uuid.UUID]membership),
		health:    make(map[uuid.UUID]health),
		listeners: concurrent.NewList(8),
		closed:    make(chan struct{}),
		closer:    make(chan struct{}, 1)}

	coll := newStorageGc(s)
	coll.start()
	return s
}

func (s *storage) Close() error {
	select {
	case <-s.closed:
		return ClosedError
	case s.closer <- struct{}{}:
	}

	for _, l := range s.Listeners() {
		close(l)
	}

	close(s.closed)
	s.wait.Wait()
	return nil
}

func (d *storage) View(fn func(*view)) {
	d.items.Read(func(data amoeba.View) {
		fn(&view{data, d.roster, d.health, time.Now()})
	})
}

func (d *storage) Update(fn func(*update) error) (err error) {
	select {
	case <-d.closed:
		return ClosedError
	default:
	}

	var ret []item
	d.items.Update(func(data amoeba.Update) {
		update := &update{&view{data, d.roster, d.health, time.Now()}, data, make([]item, 0, 8)}
		defer func() {
			ret = update.items
		}()

		err = fn(update)
	})

	// do not allow listeners to be closed while we're brodcasting.
	select {
	case <-d.closed:
		return ClosedError
	case d.closer<-struct{}{}:
	}
	defer func() {<-d.closer}()

	// Because this is outside of update, ordering is no longer guaranteed.
	// Consumers must be idempotent
	for _, ch := range d.Listeners() {
		select {
		case <-d.closed:
		case ch <- ret:
		default:
			// drop
		}
	}

	return
}

func (s *storage) Roster() (ret map[uuid.UUID]membership) {
	ret = make(map[uuid.UUID]membership)
	s.View(func(v *view) {
		for k, v := range v.Roster {
			ret[k] = v
		}
	})
	return
}

func (s *storage) Health() (ret map[uuid.UUID]health) {
	ret = make(map[uuid.UUID]health)
	s.View(func(v *view) {
		for k, v := range v.Health {
			ret[k] = v
		}
	})
	return
}

func (s *storage) Listeners() (ret []chan []item) {
	all := s.listeners.All()
	ret = make([]chan []item, 0, len(all))
	for _, l := range all {
		ret = append(ret, l.(chan []item))
	}
	return
}

func (s *storage) Listen() chan []item {
	ret := make(chan []item, 128)
	s.listeners.Append(ret)
	return ret
}

func (d *storage) All() (ret []item) {
	ret = make([]item, 0, d.items.Size())
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

func (u *view) GetLatest(id uuid.UUID, key string) (ret item, found bool) {
	status, ok := u.Roster[id]
	if !ok {
		return
	}

	rawVal, rawFound := storageGet(u.raw, id, status.Version, key)
	if !rawFound {
		return
	}

	return item{id, status.Version, key, rawVal.Val, rawVal.Ver, false, rawVal.Time}, true
}

func (u *view) GetActive(id uuid.UUID, key string) (ret item, found bool) {
	status, ok := u.Roster[id]
	if !ok || !status.Active {
		return
	}

	rawVal, rawFound := storageGet(u.raw, id, status.Version, key)
	if !rawFound || rawVal.Del {
		return
	}

	return item{id, status.Version, key, rawVal.Val, rawVal.Ver, false, rawVal.Time}, true
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

func (u *update) Put(memId uuid.UUID, memVer int, key string, keyVal string, keyVer int) bool {
	ok := storagePut(u.raw, u.now, memId, memVer, key, keyVal, keyVer)
	if !ok {
		return false
	}

	u.items = append(u.items, item{memId, memVer, key, keyVal, keyVer, false, u.now})
	switch key {
	default:
		return true
	case memberMembershipAttr:
		cur, ok := u.Roster[memId]
		if ok {
			if cur.Version >= memVer {
				return false
			}
		}

		u.Roster[memId] = membership{memId, memVer, true, u.now}
		return true
	case memberHealthAttr:
		cur, ok := u.Health[memId]
		if ok {
			if cur.Version >= memVer {
				return false
			}
		}

		u.Health[memId] = health{memId, memVer, true, u.now}
		return true
	}
}

func (u *update) Del(memId uuid.UUID, memVer int, key string, keyVer int) bool {
	ok := storageDel(u.raw, u.now, memId, memVer, key, keyVer)
	if !ok {
		return false
	}

	u.items = append(u.items, item{memId, memVer, key, "", keyVer, true, u.now})
	switch key {
	default:
		return true
	case memberMembershipAttr:
		cur, ok := u.Roster[memId]
		if ok {
			if cur.Version > memVer {
				return false
			}
		}

		u.Roster[memId] = membership{memId, memVer, false, u.now}
		return true
	case memberHealthAttr:
		cur, ok := u.Health[memId]
		if ok {
			if cur.Version > memVer {
				return false
			}
		}

		u.Health[memId] = health{memId, memVer, false, u.now}
		return true
	}
}

func (u *update) Join(id uuid.UUID, ver int) bool {
	if !u.Put(id, ver, memberMembershipAttr, "true", 0) {
		return false
	}

	// This shouldn't be able to return false...panic??
	return u.Put(id, ver, memberHealthAttr, "true", 0)
}

func (u *update) Evict(id uuid.UUID, ver int) bool {
	return u.Del(id, ver, memberMembershipAttr, 0)
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
	return fmt.Sprintf("/key:%v/id:%v/ver:%v", k.Attr, k.MemId, k.MemVer)
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
func storageGet(data amoeba.View, memId uuid.UUID, memVer int, key string) (ret storageValue, found bool) {
	raw := data.Get(storageKey{key, memId, memVer})
	if raw == nil {
		return
	}

	return raw.(storageValue), true
}

func storagePut(data amoeba.Update, time time.Time, memId uuid.UUID, memVer int, key string, keyVal string, keyVer int) bool {
	if cur, found := storageGet(data, memId, memVer, key); found {
		if keyVer <= cur.Ver {
			return false
		}
	}

	data.Put(storageKey{key, memId, memVer}, storageValue{keyVer, keyVal, false, time})
	return true
}

func storageDel(data amoeba.Update, time time.Time, memId uuid.UUID, memVer int, key string, keyVer int) bool {
	if cur, found := storageGet(data, memId, memVer, key); found {
		if keyVer < cur.Ver {
			return false
		}

		if cur.Del {
			return false
		}
	}

	data.Put(storageKey{key, memId, memVer}, storageValue{keyVer, "", true, time})
	return true
}

func storageScan(data amoeba.View, fn func(amoeba.Scan, storageKey, storageValue)) {
	data.Scan(func(s amoeba.Scan, k amoeba.Key, i interface{}) {
		key := k.(storageKey)
		val := i.(storageValue)
		fn(s, key, val)
	})
}

func storageRosterCollect(roster map[uuid.UUID]membership, fn func(uuid.UUID, membership) bool) []uuid.UUID {
	if len(roster) == 0 {
		return []uuid.UUID{}
	}

	ret := make([]uuid.UUID, 0, len(roster))
	for id, m := range roster {
		if fn(id, m) {
			ret = append(ret, id)
		}
	}
	return ret
}

func storageHealthCollect(health map[uuid.UUID]health, fn func(uuid.UUID, health) bool) []uuid.UUID {
	if len(health) == 0 {
		return []uuid.UUID{}
	}

	ret := make([]uuid.UUID, 0, len(health))
	for id, m := range health {
		if fn(id, m) {
			ret = append(ret, id)
		}
	}
	return ret
}

// A simple garbage collector for the storage api
type storageGc struct {
	store  *storage
	logger common.Logger
	gcExp  time.Duration
	gcPer  time.Duration
}

func newStorageGc(store *storage) *storageGc {
	conf := store.ctx.Config()
	c := &storageGc{
		store:  store,
		logger: store.logger.Fmt("Gc"),
		gcExp:  conf.OptionalDuration("convoy.storage.gc.expiration", 30*time.Minute),
		gcPer:  conf.OptionalDuration("convoy.storage.gc.cycle", time.Minute),
	}

	return c
}

func (c *storageGc) start() {
	c.store.wait.Add(1)
	go c.run()
}

func (d *storageGc) run() {
	defer d.store.wait.Done()
	defer d.logger.Debug("GC shutting down")

	d.logger.Debug("Running GC every [%v] with expiration [%v]", d.gcPer, d.gcExp)

	ticker := time.Tick(d.gcPer)
	for {
		select {
		case <-d.store.closed:
			return
		case <-ticker:
			d.runGcCycle(d.gcExp)
		}
	}
}

func (d *storageGc) runGcCycle(gcExp time.Duration) {
	d.store.Update(func(u *update) error {
		d.logger.Debug("Starting GC cycle [%v] for items older than [%v]", u.Time(), gcExp)

		dead := collectDeadMembers(u.Roster, u.now, gcExp)
		items1 := collectMemberItems(u.view, dead)
		items2 := collectDeadItems(u.view, u.Time(), gcExp)

		deleteDeadItems(u.raw, items1)
		deleteDeadItems(u.raw, items2)
		deleteDeadMembers(u, dead)

		d.logger.Info("Summary: Collected [%v] members and [%v] items", len(dead), len(items1)+len(items2))
		return nil
	})
}

func deleteDeadItems(u amoeba.Update, items []item) {
	for _, i := range items {
		u.Del(storageKey{i.Key, i.MemId, i.MemVer})
	}
}

func deleteDeadMembers(u *update, dead map[uuid.UUID]struct{}) {
	for k, _ := range dead {
		delete(u.Roster, k)
		delete(u.Health, k)
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

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
	memberEnabledAttr = "Convoy.Member.Enabled"
	memberHostAttr    = "Convoy.Member.Host"
	memberPortAttr    = "Convoy.Member.Port"
	memberHealthAttr  = "Convoy.Member.Status"
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

// member status
type status struct {
	Version int
	Enabled bool
	Since   time.Time
}

func (s status) String() string {
	var str string
	if s.Enabled {
		str = "Joined"
	} else {
		str = "Left"
	}

	return fmt.Sprintf("%v(%v)", str, s.Version)
}

// the core storage type
type storage struct {
	Context common.Context
	Logger  common.Logger
	Data    amoeba.Index
	Roster  map[uuid.UUID]status // sync'ed with data lock
	Wait    sync.WaitGroup
	Closed  chan struct{}
	Closer  chan struct{}
}

func newStorage(ctx common.Context, logger common.Logger) *storage {
	s := &storage{
		Context: ctx,
		Logger:  logger.Fmt("Storage"),
		Data:    amoeba.NewBTreeIndex(32),
		Roster:  make(map[uuid.UUID]status),
		Closed:  make(chan struct{}),
		Closer:  make(chan struct{}, 1)}

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
	d.Data.Read(func(data amoeba.View) {
		for k, v := range d.Roster {
			ret[k] = v
		}
	})
	return
}

func (d *storage) View(fn func(*view)) {
	d.Data.Read(func(data amoeba.View) {
		fn(&view{data, d.Roster, time.Now()})
	})
}

func (d *storage) Update(fn func(*update)) (ret []item) {
	d.Data.Update(func(data amoeba.Update) {
		update := &update{&view{data, d.Roster, time.Now()}, data, make([]item, 0, 8)}
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
	Raw    amoeba.View
	Roster map[uuid.UUID]status
	Now    time.Time
}

func (u *view) Time() time.Time {
	return u.Now
}

func (u *view) Status(id uuid.UUID) (ret status, ok bool) {
	ret, ok = u.Roster[id]
	return
}

func (u *view) GetLive(id uuid.UUID, attr string) (ret item, found bool) {
	status, ok := u.Status(id)
	if !ok || !status.Enabled {
		return
	}

	rawVal, rawFound := storageGet(u.Raw, id, status.Version, attr)
	if !rawFound || rawVal.Del {
		return
	}

	return item{id, status.Version, attr, rawVal.Val, rawVal.Ver, false, rawVal.Time}, true
}

func (u *view) ScanAll(fn func(amoeba.Scan, item)) {
	storageScan(u.Raw, func(s amoeba.Scan, k storageKey, v storageValue) {
		fn(s, item{k.MemId, k.MemVer, k.Attr, v.Val, v.Ver, v.Del, v.Time})
	})
}

func (u *view) ScanLive(fn func(amoeba.Scan, item)) {
	u.ScanAll(func(s amoeba.Scan, i item) {
		if i.Del {
			return
		}

		status, found := u.Status(i.MemId)
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
	Raw   amoeba.Update
	Items []item
}

func (u *update) Put(memId uuid.UUID, memVer int, attr string, attrVal string, attrVer int) bool {
	ok := storagePut(u.Raw, u.Now, memId, memVer, attr, attrVal, attrVer)
	if !ok {
		return false
	}

	u.Items = append(u.Items, item{memId, memVer, attr, attrVal, attrVer, false, u.Now})
	if attr != memberEnabledAttr {
		return true
	}

	stat, ok := u.Status(memId)
	if ok {
		if stat.Version >= attrVer {
			return false
		}
	}

	u.Roster[memId] = status{memVer, true, u.Now}
	return true
}

func (u *update) Del(memId uuid.UUID, memVer int, attr string, attrVer int) bool {
	ok := storageDel(u.Raw, u.Now, memId, memVer, attr, attrVer)
	if !ok {
		return false
	}

	u.Items = append(u.Items, item{memId, memVer, attr, "", attrVer, true, u.Now})
	if attr != memberEnabledAttr {
		return true
	}

	stat, ok := u.Status(memId)
	if ok {
		if stat.Version > attrVer {
			return false
		}
	}

	u.Roster[memId] = status{memVer, false, u.Now}
	return true
}

func (u *update) Enable(id uuid.UUID, ver int) bool {
	return u.Put(id, ver, memberEnabledAttr, "true", 0)
}

func (u *update) Disable(id uuid.UUID, ver int) bool {
	return u.Del(id, ver, memberEnabledAttr, 0)
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
		deleteDeadItems(u.Raw, collectMemberItems(u.view, collectDeadMembers(u.Roster, u.Time(), gcExp)))
		deleteDeadItems(u.Raw, collectDeadItems(u.view, u.Time(), gcExp))
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

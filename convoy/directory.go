package convoy

import (
	"fmt"
	"sync"

	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// System reserved keys.  Consumers should consider the: /Convoy/
// namespace off limits!
const (
	memberHostAttr   = "/Convoy/Host"
	memberPortAttr   = "/Convoy/Port"
	memberStatusAttr = "/Convoy/Status"
)

// Creating a quick lookup table to filter out system values when
// encountered in consumer contexts.
var (
	memberAttrs = map[string]struct{}{
		memberHostAttr:   struct{}{},
		memberPortAttr:   struct{}{},
		memberStatusAttr: struct{}{}}
)

// The directory is the core storage engine of the convoy replicas.
// It's primary purpose is to maintain 1.) the listing of members
// and 2.) allow searchable access to the members' datastores.
type event interface {
	scribe.Writable
	Apply(*update) bool
}

// the core storage type.
type directory struct {
	Ctx  common.Context
	Lock sync.RWMutex
	Data amoeba.Indexer
}

func newDirectory(ctx common.Context) *directory {
	return &directory{
		Ctx:  ctx,
		Data: amoeba.NewIndexer(ctx)}
}

func (d *directory) Close() error {
	d.Lock.Lock()
	defer d.Lock.Unlock()
	defer d.Data.Close()
	return nil
}

func (d *directory) Update(fn func(*update)) {
	d.Lock.Lock()
	defer d.Lock.Unlock()
	d.Data.Update(func(data amoeba.Update) {
		fn(&update{Data: data})
	})
}

func (d *directory) View(fn func(*view)) {
	d.Lock.RLock()
	defer d.Lock.RUnlock()
	d.Data.Update(func(data amoeba.Update) {
		fn(&view{Data: data})
	})
}

func (d *directory) Apply(e event) bool {
	var ret bool
	d.Update(func(u *update) {
		ret = e.Apply(u)
	})
	return ret
}

func (d *directory) ApplyAll(events []event) []bool {
	ret := make([]bool, 0, len(events))
	d.Update(func(u *update) {
		for _, e := range events {
			ret = append(ret, e.Apply(u))
		}
	})
	return ret
}

func (d *directory) Events() []event {
	events := make([]event, 0, 1024)
	d.View(func(v *view) {
		// Scan the data index
		v.Data.Scan(func(s *amoeba.Scan, k amoeba.Key, i amoeba.Item) {
			// unpack the key
			attr, id := dirUnpackAmoebaKey(k)

			// unpack the item
			val, ver, ok := dirUnpackAmoebaItem(i)

			// finally, add the event
			events = append(events,
				&dataEvent{
					Id:   id,
					Attr: attr,
					Ver:  ver,
					Val:  val,
					Del:  !ok})
		})
	})
	return events
}

// data index key type
type ai struct {
	Attr string
	Id   uuid.UUID
}

func (k ai) String() string {
	return fmt.Sprintf("/attr:%v/id:%v", k.Attr, k.Id)
}

func (k ai) IncrementAttr() ai {
	return ai{amoeba.IncrementString(k.Attr), k.Id}
}

func (k ai) IncrementId() ai {
	return ai{k.Attr, amoeba.IncrementUUID(k.Id)}
}

func (k ai) Compare(other amoeba.Sortable) int {
	o := other.(ai)
	if ret := amoeba.CompareStrings(k.Attr, o.Attr); ret != 0 {
		return ret
	}
	return amoeba.CompareUUIDs(k.Id, o.Id)
}

// standard retrievals and transformations
func dirIsReservedAttr(attr string) bool {
	_, ok := memberAttrs[attr]
	return ok
}

func dirUnpackAmoebaKey(k amoeba.Key) (attr string, id uuid.UUID) {
	ai := k.(ai)
	return ai.Attr, ai.Id
}

func dirUnpackAmoebaItem(item amoeba.Item) (val string, ver int, ok bool) {
	if item == nil {
		return
	}

	raw := item.Val()
	if raw == nil {
		return
	}

	return raw.(string), item.Ver(), true
}

func dirGetMemberAttr(data amoeba.View, id uuid.UUID, attr string) (val string, ver int, ok bool) {
	return dirUnpackAmoebaItem(data.Get(ai{attr, id}))
}

func dirAddMemberAttr(data amoeba.Update, id uuid.UUID, attr string, val string, ver int) bool {
	return data.Put(ai{attr, id}, val, ver)
}

func dirDelMemberAttr(data amoeba.Update, id uuid.UUID, attr string, ver int) bool {
	return data.Del(ai{attr, id}, ver)
}

func dirScan(data amoeba.View, fn func(*amoeba.Scan, uuid.UUID, string, string, int)) {
	data.Scan(func(s *amoeba.Scan, k amoeba.Key, i amoeba.Item) {
		attr, id := dirUnpackAmoebaKey(k)
		val, ver, ok := dirUnpackAmoebaItem(i)
		if !ok {
			return
		}

		fn(s, id, attr, val, ver)
	})
}

func dirGetMember(data amoeba.View, id uuid.UUID) *member {
	host, ver, found := dirGetMemberAttr(data, id, memberHostAttr)
	if !found {
		return nil
	}

	port, _, found := dirGetMemberAttr(data, id, memberPortAttr)
	if !found {
		return nil
	}

	return &member{
		Id:      id,
		Host:    host,
		Port:    port,
		Version: ver}
}

func dirAddMember(data amoeba.Update, m *member) bool {
	if ! dirAddMemberAttr(data, m.Id, memberHostAttr, m.Host, m.Version) {
		return false
	}

	return dirAddMemberAttr(data, m.Id, memberPortAttr, m.Port, m.Version)
}

func dirDelMember(u *update, id uuid.UUID, ver int) bool {
	type item struct {
		key  amoeba.Key
		item amoeba.Item
	}

	// see if someone else has already done this work.
	if ! u.Data.Del(ai{memberHostAttr, id}, ver) {
		return false
	}

	deadItems := make([]item, 0, 128)
	u.Data.Scan(func(s *amoeba.Scan, k amoeba.Key, i amoeba.Item) {
		ki := k.(ai)
		if ki.Id == id {
			deadItems = append(deadItems, item{k, i})
			return
		}
	})

	for _, i := range deadItems {
		u.Data.Del(i.key, i.item.Ver())
	}

	return true
}

// A couple very simple low level view/update abstractions
type view struct {
	Data amoeba.View
}

func (u *view) GetMember(id uuid.UUID) *member {
	return dirGetMember(u.Data, id)
}

func (u *view) GetMemberAttr(id uuid.UUID, attr string) (string, int, bool) {
	return dirGetMemberAttr(u.Data, id, attr)
}

func (u *view) Scan(fn func(scan *amoeba.Scan, id uuid.UUID, attr string, val string, ver int)) {
	dirScan(u.Data, fn)
}

type update struct {
	view
	Data amoeba.Update
}

func (u *update) AddMemberAttr(id uuid.UUID, attr string, val string, ver int) {
	dirAddMemberAttr(u.Data, id, attr, val, ver)
}

func (u *update) DelMemberAttr(id uuid.UUID, attr string, ver int) {
	dirDelMemberAttr(u.Data, id, attr, ver)
}

func (u *update) AddMember(m *member) {
	dirAddMember(u.Data, m)
}

func (u *update) DelMember(id uuid.UUID, ver int) {
	dirDelMember(u, id, ver)
}


func readEvent(r scribe.Reader) (event, error) {
	var typ string
	if err := r.Read("type", &typ); err != nil {
		return nil, err
	}

	switch typ {
	default:
		return nil, fmt.Errorf("Cannot parse event.  Unknown type [%v]", typ)
	case "data":
		return readDataEvent(r)
	case "member":
		return readMemberEvent(r)
	}
}

func writeEvent(w scribe.Writer, e event) {
	e.Write(w)

	switch e.(type) {
	case *dataEvent:
		w.Write("type", "data")
	case *memberEvent:
		w.Write("type", "member")
	}
}

// The primary data event type.
type dataEvent struct {
	Id   uuid.UUID
	Attr string
	Val  string
	Ver  int
	Del  bool
}

func readDataEvent(r scribe.Reader) (*dataEvent, error) {
	id, err := scribe.ReadUUID(r, "id")
	if err != nil {
		return nil, err
	}

	event := &dataEvent{Id: id}
	if err := r.Read("attr", &event.Attr); err != nil {
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

func (e *dataEvent) Write(w scribe.Writer) {
	w.Write("id", e.Id.String())
	w.Write("attr", e.Attr)
	w.Write("val", e.Val)
	w.Write("ver", e.Ver)
	w.Write("del", e.Del)
}

func (e *dataEvent) Apply(tx *update) bool {
	if e.Del {
		tx.DelMemberAttr(e.Id, e.Attr, e.Ver)
		return true
	} else {
		tx.AddMemberAttr(e.Id, e.Attr, e.Val, e.Ver)
		return true
	}
}

// a member add/leave
type memberEvent struct {
	Id   uuid.UUID
	Host string
	Port string
	Ver  int
	Del  bool
}

func readMemberEvent(r scribe.Reader) (*memberEvent, error) {
	id, err := scribe.ReadUUID(r, "id")
	if err != nil {
		return nil, err
	}

	event := &memberEvent{Id: id}
	if err := r.Read("host", &event.Host); err != nil {
		return nil, err
	}
	if err := r.Read("port", &event.Port); err != nil {
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

func (e *memberEvent) Write(w scribe.Writer) {
	w.Write("id", e.Id.String())
	w.Write("host", e.Host)
	w.Write("port", e.Port)
	w.Write("ver", e.Ver)
	w.Write("del", e.Del)
}

func (e *memberEvent) Apply(tx *update) bool {
	if e.Del {
		tx.DelMember(e.Id, e.Ver)
		return true
	} else {
		tx.AddMember(newMember(e.Id, e.Host, e.Port, e.Ver))
		return true
	}
}

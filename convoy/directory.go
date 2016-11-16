package convoy

import (
	"fmt"
	"sync"

	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/enc"
	uuid "github.com/satori/go.uuid"
)

// The directory is the core storage engine of the convoy replicas.
// It's primary purpose is to maintain 1.) the listing of members
// and 2.) allow searchable access to the members' datastores.

// Events encapsulate a specific mutation, allowing them to
// be stored or replicated for future use.  Events are stored
// in time sorted order - but are NOT made durable.  Therefore,
// this is NOT suitable for disaster recovery.  The current
// approach is to recreate the directory from a "live" member.
// The primary eventing abstraction.  The directory can be
// expressed as a sequence of events and likewise can be
// be built from them.
type event interface {
	enc.Writable
	Apply(*update)
}

// the core storage type.
type directory struct {
	Ctx     common.Context
	Lock    sync.RWMutex
	Data    amoeba.Indexer
	Members amoeba.Indexer
}

func newDirectory(ctx common.Context) *directory {
	return &directory{
		Ctx:     ctx,
		Data:    amoeba.NewIndexer(ctx),
		Members: amoeba.NewIndexer(ctx)}
}

func (d *directory) Close() error {
	d.Lock.Lock()
	defer d.Lock.Lock()
	defer d.Members.Close()
	defer d.Data.Close()
	return nil
}

func (d *directory) Update(fn func(*update)) {
	d.Lock.Lock()
	defer d.Lock.Lock()
	d.Members.Update(func(members amoeba.Update) {
		d.Data.Update(func(data amoeba.Update) {
			fn(&update{Data: data, Members: members})
		})
	})
}

func (d *directory) View(fn func(*view)) {
	d.Lock.RLock()
	defer d.Lock.RUnlock()
	d.Members.Update(func(members amoeba.Update) {
		d.Data.Update(func(data amoeba.Update) {
			fn(&view{Data: data, Members: members})
		})
	})
}

func (d *directory) Apply(e event) {
	d.Update(func(u *update) {
		e.Apply(u)
	})
}

func (d *directory) ApplyAll(events []event) {
	d.Update(func(u *update) {
		for _, e := range events {
			e.Apply(u)
		}
	})
}

func (d *directory) Events() []event {
	events := make([]event, 0, 1024)
	d.View(func(v *view) {
		// Scan the data index
		v.Data.Scan(func(s *amoeba.Scan, k amoeba.Key, i amoeba.Item) {
			key := k.(ki)
			val := i.Val()
			del := val == nil
			events = append(events,
				&dataEvent{
					Id:  key.Id,
					Key: key.Key,
					Ver: i.Ver(),
					Val: val.(string),
					Del: del})
		})

		// Scan the data index
		v.Members.Scan(func(s *amoeba.Scan, k amoeba.Key, i amoeba.Item) {
			key := k.(mi)
			if val := i.Val(); val == nil {
				events = append(events,
					&memberEvent{
						Id:  key.Id,
						Ver: i.Ver()})
				return
			}

			mem := i.Val().(*member)
			events = append(events,
				&memberEvent{
					Id:   key.Id,
					Host: mem.Host,
					Port: mem.Port,
					Ver:  mem.Version})
		})
	})
	return events
}

// member index key type
type mi struct {
	Id uuid.UUID
}

func (m mi) Compare(other amoeba.Sortable) int {
	return amoeba.CompareUUIDs(m.Id, other.(mi).Id)
}

// data index key type
type ki struct {
	Key string
	Id  uuid.UUID
}

func (k ki) String() string {
	return fmt.Sprintf("/key:%v/id:%v", k.Key, k.Id)
}

func (k ki) IncrementKey() ki {
	return ki{amoeba.IncrementString(k.Key), k.Id}
}

func (k ki) IncrementId() ki {
	return ki{k.Key, amoeba.IncrementUUID(k.Id)}
}

func (k ki) Compare(other amoeba.Sortable) int {
	o := other.(ki)
	if ret := amoeba.CompareStrings(k.Key, o.Key); ret != 0 {
		return ret
	}
	return amoeba.CompareUUIDs(k.Id, o.Id)
}

// A couple very simple low level view/update abstractions
type view struct {
	Members amoeba.View
	Data    amoeba.View
}

type update struct {
	Members amoeba.Update
	Data    amoeba.Update
}

func (u *update) AddDatum(id uuid.UUID, key string, val string, ver int) {
	u.Data.Put(ki{key, id}, val, ver)
}

func (u *update) DelDatum(id uuid.UUID, key string, ver int) {
	u.Data.Del(ki{key, id}, ver)
}

func (u *update) AddMember(id uuid.UUID, m *member) {
	u.Members.Put(mi{id}, m, m.Version)
}

func (u *update) DelMember(id uuid.UUID, ver int) {
	u.Members.Del(mi{id}, ver)

	type item struct {
		key  amoeba.Key
		item amoeba.Item
	}

	deadItems := make([]item, 0, 128)
	u.Data.Scan(func(s *amoeba.Scan, k amoeba.Key, i amoeba.Item) {
		ki := k.(ki)
		if ki.Id == id {
			deadItems = append(deadItems, item{k, i})
			return
		}
	})

	for _, i := range deadItems {
		u.Data.Del(i.key, i.item.Ver())
	}
}


// The primary data event type.
type dataEvent struct {
	Id  uuid.UUID
	Key string
	Val string
	Ver int
	Del bool
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

func (e *dataEvent) Apply(tx *update) {
	if e.Del {
		tx.DelDatum(e.Id, e.Key, e.Ver)
	} else {
		tx.AddDatum(e.Id, e.Key, e.Val, e.Ver)
	}
}

type memberEvent struct {
	Id   uuid.UUID
	Host string
	Port int
	Ver  int
	Del  bool
}

func readAddMemberEvent(r enc.Reader) (*memberEvent, error) {
	id, err := readUUID(r, "id")
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

func (e *memberEvent) Write(w enc.Writer) {
	w.Write("id", e.Id.String())
	w.Write("host", e.Host)
	w.Write("port", e.Port)
	w.Write("ver", e.Ver)
	w.Write("del", e.Del)
}

func (e *memberEvent) Apply(tx *update) {
	if e.Del {
		tx.DelMember(e.Id, e.Ver)
	} else {
		tx.AddMember(e.Id, newMember(e.Id, e.Host, e.Port, e.Ver))
	}
}


package convoy

import (
	"hash/fnv"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// System reserved keys.  Consumers should consider the: /Convoy/
// namespace off limits!
const (
	memberHostAttr   = "/Convoy/Member/Host"
	memberPortAttr   = "/Convoy/Member/Port"
	memberStatusAttr = "/Convoy/Member/Status"
)

// Creating a quick lookup table to filter out system values when
// encountered in consumer contexts.
var (
	memberAttrs = map[string]struct{}{
		memberHostAttr:   struct{}{},
		memberPortAttr:   struct{}{},
		memberStatusAttr: struct{}{}}
)

// Reads from the channel of events and applies them to the directory.
func dirIndexEvents(ch <-chan event, dir *directory) {
	go func() {
		for e := range ch {
			done, timeout := concurrent.NewBreaker(365*24*time.Hour, func() interface{} {
				dir.Apply(e)
				return nil
			})

			select {
			case <-done:
				continue
			case <-timeout:
				return
			}
		}
	}()
}

// The directory is the core storage engine of the convoy replicas.
// Its primary purpose is to maintain 1.) the listing of members
// and 2.) allow searchable access to the members' datastores.
type event interface {
	scribe.Writable
	Apply(*dirUpdate) bool
}

// the core storage type.
type directory struct {
	Data   amoeba.Indexer
	Digest []byte
}

func newDirectory(ctx common.Context) *directory {
	return &directory{
		Data: amoeba.NewIndexer(ctx)}
}

func (d *directory) Close() error {
	return d.Data.Close()
}

func (d *directory) update(fn func(*dirUpdate)) {
	d.Data.Update(func(data amoeba.Update) {
		fn(&dirUpdate{Data: data})
	})
}

func (d *directory) View(fn func(*dirView)) {
	d.Data.Read(func(data amoeba.View) {
		fn(&dirView{Data: data})
	})
}

func (d *directory) Collect(filter func(uuid.UUID, string, string, int) bool) (ret []*member) {
	ret = make([]*member, 0)
	d.View(func(v *dirView) {
		ret = v.Collect(filter)
	})
	return
}

func (d *directory) First(filter func(uuid.UUID, string, string, int) bool) (ret *member) {
	d.View(func(v *dirView) {
		ret = v.First(filter)
	})
	return
}

func (d *directory) All() (ret []*member) {
	return d.Collect(func(uuid.UUID, string, string, int) bool {
		return true
	})
	return
}

func (d *directory) Size() int {
	return d.Data.Size()
}

func (d *directory) Hash() []byte {
	hash := fnv.New64()

	d.View(func(v *dirView) {
		v.Scan(func(s *amoeba.Scan, id uuid.UUID, key string, val string, ver int) {
			hash.Write(id.Bytes())
			binary.Write(hash, binary.BigEndian, key)
			binary.Write(hash, binary.BigEndian, val)
			binary.Write(hash, binary.BigEndian, ver)
		})
	})

	return hash.Sum(nil)
}

func (d *directory) Apply(e event) (ret bool) {
	d.update(func(u *dirUpdate) {
		ret = e.Apply(u)
	})
	return
}

func (d *directory) ApplyAll(events []event) (ret []bool) {
	ret = make([]bool, 0, len(events))
	d.update(func(u *dirUpdate) {
		for _, e := range events {
			ret = append(ret, e.Apply(u))
		}
	})
	return
}

func (d *directory) Events() (events []event) {
	d.View(func(v *dirView) {
		events = dirEvents(v.Data)
	})
	return
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

func dirEvents(data amoeba.View) (events []event) {
	events = make([]event, 0, 1024)
	data.Scan(func(s *amoeba.Scan, k amoeba.Key, i amoeba.Item) {
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
	return
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
	if !dirAddMemberAttr(data, m.Id, memberHostAttr, m.Host, m.Version) {
		return false
	}

	return dirAddMemberAttr(data, m.Id, memberPortAttr, m.Port, m.Version)
}

func dirDelMember(data amoeba.Update, id uuid.UUID, ver int) bool {
	type item struct {
		key  amoeba.Key
		item amoeba.Item
	}

	deadItems := make([]item, 0, 128)
	data.Scan(func(s *amoeba.Scan, k amoeba.Key, i amoeba.Item) {
		ki := k.(ai)
		if ki.Id == id {
			deadItems = append(deadItems, item{k, i})
			return
		}
	})

	for _, i := range deadItems {
		data.Del(i.key, i.item.Ver())
	}

	return true
}

func dirFirst(v amoeba.View, filter func(uuid.UUID, string, string, int) bool) (member *member) {
	dirScan(v, func(s *amoeba.Scan, id uuid.UUID, key string, val string, ver int) {
		if filter(id, key, val, ver) {
			defer s.Stop()
			member = dirGetMember(v, id)
		}
	})
	return
}

func dirCollect(v amoeba.View, filter func(uuid.UUID, string, string, int) bool) (members []*member) {
	ids := make(map[uuid.UUID]struct{})

	dirScan(v, func(s *amoeba.Scan, id uuid.UUID, key string, val string, ver int) {
		if filter(id, key, val, ver) {
			ids[id] = struct{}{}
		}
	})

	members = make([]*member, 0, len(ids))

	for id, _ := range ids {
		m := dirGetMember(v, id)
		if m != nil {
			members = append(members, m)
		}
	}

	return
}

// A couple very simple low level view/update abstractions
type dirView struct {
	Data amoeba.View
}

func (u *dirView) GetMember(id uuid.UUID) *member {
	return dirGetMember(u.Data, id)
}

func (u *dirView) GetMemberAttr(id uuid.UUID, attr string) (string, int, bool) {
	return dirGetMemberAttr(u.Data, id, attr)
}

func (u *dirView) Scan(fn func(scan *amoeba.Scan, id uuid.UUID, attr string, val string, ver int)) {
	dirScan(u.Data, fn)
}

func (u *dirView) Collect(filter func(id uuid.UUID, attr string, val string, ver int) bool) []*member {
	return dirCollect(u.Data, filter)
}

func (u *dirView) First(filter func(id uuid.UUID, attr string, val string, ver int) bool) *member {
	return dirFirst(u.Data, filter)
}

type dirUpdate struct {
	dirView
	Data amoeba.Update
}

func (u *dirUpdate) AddMemberAttr(id uuid.UUID, attr string, val string, ver int) bool {
	return dirAddMemberAttr(u.Data, id, attr, val, ver)
}

func (u *dirUpdate) DelMemberAttr(id uuid.UUID, attr string, ver int) bool {
	return dirDelMemberAttr(u.Data, id, attr, ver)
}

func (u *dirUpdate) AddMember(m *member) bool {
	return dirAddMember(u.Data, m)
}

func (u *dirUpdate) DelMember(id uuid.UUID, ver int) bool {
	return dirDelMember(u.Data, id, ver)
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
	w.Write("type", "data")
	w.Write("id", e.Id.String())
	w.Write("attr", e.Attr)
	w.Write("val", e.Val)
	w.Write("ver", e.Ver)
	w.Write("del", e.Del)
}

func (e *dataEvent) Apply(tx *dirUpdate) bool {
	if e.Del {
		return tx.DelMemberAttr(e.Id, e.Attr, e.Ver)
	} else {
		return tx.AddMemberAttr(e.Id, e.Attr, e.Val, e.Ver)
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

func addMemberEvent(m *member) event {
	return &memberEvent{m.Id, m.Host, m.Port, m.Version, false}
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
	w.Write("type", "member")
	w.Write("id", e.Id.String())
	w.Write("host", e.Host)
	w.Write("port", e.Port)
	w.Write("ver", e.Ver)
	w.Write("del", e.Del)
}

func (e *memberEvent) Apply(tx *dirUpdate) bool {
	if e.Del {
		return tx.DelMember(e.Id, e.Ver)
	} else {
		return tx.AddMember(newMember(e.Id, e.Host, e.Port, e.Ver))
	}
}

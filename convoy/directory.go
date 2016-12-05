package convoy

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"time"

	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/scribe"
	metrics "github.com/rcrowley/go-metrics"
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
				dir.Apply(e, true)
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

type dirStats struct {
	memberAdds metrics.Counter
	memberDels metrics.Counter
	memberRate metrics.Meter
}

func newDirStats() *dirStats {
	r := metrics.NewRegistry()

	return &dirStats{
		memberAdds: metrics.NewRegisteredCounter("convoy.directory.memberAdds", r),
		memberDels: metrics.NewRegisteredCounter("convoy.directory.memberDels", r),
		memberRate: metrics.NewRegisteredMeter("convoy.directory.memberRate", r),
	}
}

func (s *dirStats) String() string {
	return fmt.Sprintf("Members: Adds: %v, Dels: %v, Rate: %v", s.memberAdds.Count(), s.memberDels.Count(), s.memberRate.Rate5())
}

func (d *dirStats) Write(w scribe.Writer) {
	scribe.WriteInt(w, "MemberAdds", int(d.memberAdds.Count()))
	scribe.WriteInt(w, "MemberDels", int(d.memberDels.Count()))
	scribe.WriteInt(w, "MemberRate1m", int(d.memberRate.Rate1()))
	scribe.WriteInt(w, "MemberRate5m", int(d.memberRate.Rate5()))
}

// the core storage type.
// Invariants:
//   * Members must always have: host, port, status attributes.
//   * Members version is obtained by taking latest of host, port, status.
//   * Status events at same version have LWW semantics
type directory struct {
	Data  amoeba.Indexer
	Log   *timeLog
	Stats *dirStats
}

func newDirectory(ctx common.Context) *directory {
	return &directory{
		Data:  amoeba.NewIndexer(ctx),
		Log:   newTimeLog(ctx),
		Stats: newDirStats()}
}

func (d *directory) Close() error {
	return d.Data.Close()
}

func (d *directory) Update(fn func(*dirUpdate)) {
	d.Data.Update(func(data amoeba.Update) {
		fn(&dirUpdate{Data: data, Stats: d.Stats})
	})
}

func (d *directory) View(fn func(*dirView)) {
	d.Data.Read(func(data amoeba.View) {
		fn(&dirView{Data: data, Stats: d.Stats})
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

func (d *directory) NumMembers() int {
	return int(d.Stats.memberAdds.Count() - d.Stats.memberDels.Count())
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

func (d *directory) Apply(e event, log bool) bool {
	ret := d.ApplyAll([]event{e}, log)
	return ret[0]
}

func (d *directory) ApplyAll(events []event, log bool) (ret []bool) {
	ret = make([]bool, 0, len(events))
	d.Update(func(u *dirUpdate) {
		for _, e := range events {
			ret = append(ret, e.Apply(u))
		}

		if log {
			d.Log.Push(dirCollectSuccesses(events, ret))
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

func dirForceDelMemberAttr(data amoeba.Update, id uuid.UUID, attr string, ver int) {
	data.DelNow(ai{attr, id})
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
	host, ver1, found := dirGetMemberAttr(data, id, memberHostAttr)
	if !found {
		return nil
	}

	port, ver2, found := dirGetMemberAttr(data, id, memberPortAttr)
	if !found {
		return nil
	}

	stat, ver3, found := dirGetMemberAttr(data, id, memberStatusAttr)
	if !found {
		return nil
	}

	status, err := strconv.Atoi(stat)
	if err != nil {
		panic(err)
	}

	ver := int(math.Max(float64(ver1), math.Max(float64(ver2), float64(ver3))))
	return &member{
		Id:      id,
		Host:    host,
		Port:    port,
		Status:  MemberStatus(status),
		Version: ver}
}

func dirAddMember(data amoeba.Update, m *member) bool {
	if !dirAddMemberAttr(data, m.Id, memberStatusAttr, strconv.Itoa(int(m.Status)), m.Version) {
		return false
	}

	if !dirAddMemberAttr(data, m.Id, memberHostAttr, m.Host, m.Version) {
		return false
	}

	return dirAddMemberAttr(data, m.Id, memberPortAttr, m.Port, m.Version)
}

func dirUpdateMemberStatus(data amoeba.Update, id uuid.UUID, status MemberStatus, ver int) bool {
	_, curVer, _ := dirGetMemberAttr(data, id, memberStatusAttr)
	if ver < curVer {
		return false
	}

	dirForceDelMemberAttr(data, id, memberStatusAttr, curVer)
	dirAddMemberAttr(data, id, memberStatusAttr, strconv.Itoa(int(status)), ver)
	return true
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

func dirCollectSuccesses(all []event, success []bool) []event {
	forward := make([]event, 0, len(success))
	for i, b := range success {
		if b {
			forward = append(forward, all[i])
		}
	}

	return forward
}

// A couple very simple low level view/update abstractions
type dirView struct {
	Data  amoeba.View
	Stats *dirStats
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
	Data  amoeba.Update
	Stats *dirStats
}

func (u *dirUpdate) AddMemberAttr(id uuid.UUID, attr string, val string, ver int) bool {
	return dirAddMemberAttr(u.Data, id, attr, val, ver)
}

func (u *dirUpdate) DelMemberAttr(id uuid.UUID, attr string, ver int) bool {
	return dirDelMemberAttr(u.Data, id, attr, ver)
}

func (u *dirUpdate) AddMember(m *member) (ret bool) {
	defer func() {
		if ret {
			u.Stats.memberRate.Mark(1)
			u.Stats.memberAdds.Inc(1)
		}
	}()
	ret = dirAddMember(u.Data, m)
	return
}

func (u *dirUpdate) DelMember(id uuid.UUID, ver int) (ret bool) {
	defer func() {
		if ret {
			u.Stats.memberRate.Mark(1)
			u.Stats.memberDels.Inc(1)
		}
	}()
	ret = dirDelMember(u.Data, id, ver)
	return
}

func (u *dirUpdate) UpdateMember(id uuid.UUID, status MemberStatus, ver int) bool {
	return dirUpdateMemberStatus(u.Data, id, status, ver)
}

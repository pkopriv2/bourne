package convoy

import (
	"strconv"
	"sync"
	"time"

	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// System reserved keys.  Consumers should consider the: /Convoy/
// namespace off limits!
const ()

// Reads from the channel of events and applies them to the directory.
func dirIndexEvents(ch <-chan event, dir *directory) {
	go func() {
		for e := range ch {
			done, timeout := concurrent.NewBreaker(365*24*time.Hour, func() interface{} {
				dir.ApplyAll([]event{e})
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

// The event is the fundamental unit of change dissmenation.
type event interface {
	scribe.Writable

	// Applies the event to the storage.
	Apply(u *update) bool
}

// Retrieves a member from the directory
func dirGetMember(v *view, id uuid.UUID) *member {
	// hostItem, found := v.Get(id, memberHostAttr)
	// if !found {
	// return nil
	// }
	//
	// portItem, found := v.Get(id, memberPortAttr)
	// if !found {
	// return nil
	// }
	//
	// statItem, found := v.Get(id, memberStatusAttr)
	// if !found {
	// return nil
	// }
	//
	// status, err := strconv.Atoi(stat)
	// if err != nil {
	// panic(err)
	// }
	//
	// ver := int(math.Max(float64(ver1), math.Max(float64(ver2), float64(ver3))))
	// return &member{
	// Id:      id,
	// Host:    host,
	// Port:    port,
	// Status:  MemberStatus(status),
	// Version: ver}
	return nil
}

// the core storage type.
// Invariants:
//   * Members must always have: host, port, status attributes.
//   * Members version is obtained by taking latest of host, port, status.
//   * Status events at same version have LWW (last-win-write) semantics
type directory struct {
	Logger common.Logger
	Core   *storage

	Handlers []func([]event)
	Lock     sync.RWMutex
}

func newDirectory(ctx common.Context, logger common.Logger) *directory {
	return &directory{
		Logger: logger.Fmt("Directory"),
		Core:   newStorage(ctx, logger),
	}
}

func (t *directory) Listeners() []func([]event) {
	t.Lock.RLock()
	defer t.Lock.RUnlock()
	ret := make([]func([]event), 0, len(t.Handlers))
	for _, fn := range t.Handlers {
		ret = append(ret, fn)
	}
	return ret
}

func (e *directory) Listen(fn func([]event)) {
	e.Lock.Lock()
	defer e.Lock.Unlock()
	e.Handlers = append(e.Handlers, fn)
}

func (t *directory) Close() (ret error) {
	// ret = t.Core.Close()
	t.broadcast(nil)
	return
}

func (t *directory) broadcast(batch []event) {
	for _, fn := range t.Listeners() {
		fn(batch)
	}
}

func (d *directory) ApplyAll(events []event) []bool {
	return nil
	// return d.Core.Apply(events)
}

func (d *directory) Events() []event {
	return nil
	// return d.Core.Events()
}

func (d *directory) Get(id uuid.UUID) (ret *member) {
	d.Core.View(func(u *view) {
		ret = dirGetMember(u, id)
	})
	return
}

func (d *directory) Collect(filter func(uuid.UUID, string, string) bool) (ret []*member) {
	ret = []*member{}
	d.Core.View(func(v *view) {
		ids := make(map[uuid.UUID]struct{})

		v.ScanLive(func(s amoeba.Scan, i item) {
			if filter(i.MemId, i.Attr, i.Val) {
				ids[i.MemId] = struct{}{}
			}
		})

		ret = make([]*member, 0, len(ids))
		for id, _ := range ids {
			if m := dirGetMember(v, id); m != nil {
				ret = append(ret, m)
			}
		}
	})
	return
}

func (d *directory) First(filter func(uuid.UUID, string, string) bool) (ret *member) {
	d.Core.View(func(v *view) {
		v.ScanLive(func(s amoeba.Scan, i item) {
			if filter(i.MemId, i.Attr, i.Val) {
				defer s.Stop()
				ret = dirGetMember(v, i.MemId)
			}
		})
	})
	return
}

func (d *directory) All() (ret []*member) {
	return d.Collect(func(uuid.UUID, string, string) bool {
		return true
	})
	return
}

func (d *directory) AddMember(m *member) {
	d.Core.Update(func(u *update) {
		if ! u.Join(m.Id, m.Version) {
			return
		}

		u.Put(m.Id, m.Version, memberStatusAttr, strconv.Itoa(int(m.Status)), m.Version)
		u.Put(m.Id, m.Version, memberHostAttr, m.Host, m.Version)
		u.Put(m.Id, m.Version, memberPortAttr, m.Port, m.Version)
	})
	return
}

// The primary data event type.
type dataEvent struct {
	Id   uuid.UUID
	Attr string
	Val  string
	Ver  int
	Del  bool
}

func (e *dataEvent) Write(w scribe.Writer) {
	w.Write("id", e.Id.String())
	w.Write("attr", e.Attr)
	w.Write("val", e.Val)
	w.Write("ver", e.Ver)
	w.Write("del", e.Del)
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

func (e *dataEvent) Apply(tx *update) bool {
	return false
	// if e.Del {
	// return tx.Del(e.Id, e.Attr, e.Ver)
	// } else {
	// return tx.Put(e.Id, e.Attr, e.Val, e.Ver)
	// }
}

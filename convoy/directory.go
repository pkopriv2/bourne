package convoy

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// Reads from the channel of events and applies them to the directory.
func dirIndexEvents(ch <-chan event, dir *directory) {
	go func() {
		for e := range ch {
			done, timeout := concurrent.NewBreaker(365*24*time.Hour, func() interface{} {
				dir.Apply([]event{e})
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

// Adds a listener to the change log and returns a buffered channel of changes.
// the channel is closed when the log is closed.
func dirListen(dir *directory) <-chan []event {
	ret := make(chan []event, 1024)
	dir.Listen(func(batch []event) {
		if batch == nil {
			close(ret)
			return
		}

		ret <- batch
	})
	return ret
}

// The event is the fundamental unit of change dissmenation.
type event interface {
	scribe.Writable

	// Applies the event to the storage.
	Apply(u *update) bool
}

func readEvent(r scribe.Reader) (event, error) {
	return readItem(r)
}

type directory struct {
	Logger   common.Logger
	Core     *storage
	Handlers []func([]event)
	Lock     sync.RWMutex
}

func newDirectory(ctx common.Context, logger common.Logger) *directory {
	return &directory{
		Logger: logger.Fmt("Directory"),
		Core:   newStorage(ctx, logger),
	}
}

func (d *directory) Listeners() []func([]event) {
	d.Lock.RLock()
	defer d.Lock.RUnlock()
	ret := make([]func([]event), 0, len(d.Handlers))
	for _, fn := range d.Handlers {
		ret = append(ret, fn)
	}
	return ret
}

func (d *directory) Listen(fn func([]event)) {
	d.Lock.Lock()
	defer d.Lock.Unlock()
	d.Handlers = append(d.Handlers, fn)
}

func (d *directory) Close() (ret error) {
	ret = d.Core.Close()
	d.broadcast(nil)
	return
}

func (d *directory) broadcast(batch []event) {
	if len(batch) == 0 {
		return
	}

	for _, fn := range d.Listeners() {
		fn(batch)
	}
}

func (d *directory) Apply(events []event) (ret []bool) {
	ret = make([]bool, 0, len(events))
	d.Core.Update(func(u *update) {
		for _, e := range events {
			ret = append(ret, e.Apply(u))
		}
	})

	d.broadcast(dirCollectSuccesses(events, ret))
	return
}

func (d *directory) Events() []event {
	items := d.Core.All()

	ret := make([]event, 0, len(items))
	for _, i := range items {
		ret = append(ret, i)
	}
	return ret
}

func (d *directory) Get(id uuid.UUID) (ret member, ok bool) {
	d.Core.View(func(u *view) {
		ret, ok = dirGetActiveMember(u, id)
	})
	return
}

func (d *directory) Join(m member) (err error) {
	items := d.Core.Update(func(u *update) {
		if !u.Join(m.Id, m.Version) {
			err = errors.Errorf("Member alread joined [%v]", m)
			return
		}

		if !m.Healthy {
			err = errors.Errorf("Cannot join unhealthy member [%v]", m)
			return
		}

		u.Put(m.Id, m.Version, memberHealthAttr, "", m.Version)
		u.Put(m.Id, m.Version, memberHostAttr, m.Host, m.Version)
		u.Put(m.Id, m.Version, memberPortAttr, m.Port, m.Version)
	})

	d.broadcast(dirItemsToEvents(items))
	return
}

func (d *directory) Evict(m member) (err error) {
	items := d.Core.Update(func(u *update) {
		if !u.Evict(m.Id, m.Version) {
			err = errors.Errorf("Member already joined [%v]", m.Id)
			return
		}
	})

	d.broadcast(dirItemsToEvents(items))
	return
}

func (d *directory) Fail(m member) (err error) {
	items := d.Core.Update(func(u *update) {
		if !u.Del(m.Id, m.Version, memberHealthAttr, m.Version) {
			err = errors.Errorf("Unable to fail member [%v]", m)
		}
	})

	d.broadcast(dirItemsToEvents(items))
	return
}

func (d *directory) Collect(filter func(uuid.UUID, string, string) bool) (ret []member) {
	ret = []member{}
	d.Core.View(func(v *view) {
		ids := make(map[uuid.UUID]struct{})

		v.ScanActive(func(s amoeba.Scan, i item) {
			if filter(i.MemId, i.Attr, i.Val) {
				ids[i.MemId] = struct{}{}
			}
		})

		ret = make([]member, 0, len(ids))
		for id, _ := range ids {
			if m, ok := dirGetActiveMember(v, id); ok {
				ret = append(ret, m)
			}
		}
	})
	return
}

func (d *directory) First(filter func(uuid.UUID, string, string) bool) (ret member, ok bool) {
	d.Core.View(func(v *view) {
		v.ScanActive(func(s amoeba.Scan, i item) {
			if filter(i.MemId, i.Attr, i.Val) {
				defer s.Stop()
				ret, ok = dirGetActiveMember(v, i.MemId)
			}
		})
	})
	return
}

func (d *directory) Healthy() (ret []member) {
	d.Core.View(func(v *view) {
		ret = make([]member, 0, len(v.Health))
		for id, h := range v.Health {
			if h.Healthy {
				if m, ok := dirGetActiveMember(v, id); ok {
					ret = append(ret, m)
				}
			}
		}
	})
	return
}

func (d *directory) Unhealthy() (ret []member) {
	d.Core.View(func(v *view) {
		ret = make([]member, 0, len(v.Health))
		for id, h := range v.Health {
			if !h.Healthy {
				if m, ok := dirGetActiveMember(v, id); ok {
					ret = append(ret, m)
				}
			}
		}
	})
	return
}

func (d *directory) Active() (ret []member) {
	d.Core.View(func(v *view) {
		ret = make([]member, 0, len(v.Health))
		for id, m := range v.Roster {
			if m.Active {
				if m, ok := dirGetActiveMember(v, id); ok {
					ret = append(ret, m)
				}
			}
		}
	})
	return
}

func (d *directory) Evicted() (ret []member) {
	d.Core.View(func(v *view) {
		ret = make([]member, 0, len(v.Health))
		for id, m := range v.Roster {
			if !m.Active {
				if m, ok := dirGetMember(v, id); ok {
					ret = append(ret, m)
				}
			}
		}
	})
	return
}

// Retrieves an active member from the directory.  To be active,
// means that no eviction has been seen for this memeber.
func dirGetActiveMember(v *view, id uuid.UUID) (member, bool) {
	if m, ok := dirGetMember(v, id);  ok {
		if m.Active {
			return m, true
		}
	}
	return member{}, false
}

// Retrieves the latest member
func dirGetMember(v *view, id uuid.UUID) (member, bool) {
	hostItem, found := v.GetLatest(id, memberHostAttr)
	if !found {
		return member{}, false
	}

	portItem, found := v.GetLatest(id, memberPortAttr)
	if !found {
		return member{}, false
	}

	mem, _ := v.Roster[id]
	if mem.Version != hostItem.Ver {
		return member{}, false
	}

	health, _ := v.Health[id]
	if health.Version != hostItem.Ver {
		return member{}, false
	}

	return member{
		Id:      id,
		Host:    hostItem.Val,
		Port:    portItem.Val,
		Healthy: health.Healthy,
		Active:  mem.Active,
		Version: hostItem.Ver}, true
}

func dirCollectSuccesses(events []event, success []bool) []event {
	if len(events) != len(success) {
		panic("Unequal array length")
	}

	ret := make([]event, 0, len(events))
	for i, s := range success {
		if s {
			ret = append(ret, events[i])
		}
	}

	return ret
}

func dirItemsToEvents(items []item) []event {
	ret := make([]event, 0, len(items))
	for _, i := range items {
		ret = append(ret, i)
	}
	return ret
}

package convoy

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

var (
	dirClosedError = errors.New("DIR:CLOSED")
)

// Reads from the channel of events and applies them to the directory.
func dirIndexEvents(ch <-chan event, dir *directory) {
	go func() {
		for e := range ch {
			dir.Apply([]event{e})
		}
	}()
}

// Adds a listener to the change log and returns a buffered channel of changes.
// the channel is closed when the log is closed.
func dirListen(dir *directory) <-chan []event {
	ret := make(chan []event, 1024)
	dir.OnChange(func(batch []event) {
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
	logger common.Logger
	Core   *storage
	lock   sync.RWMutex
	closed bool
}

func newDirectory(ctx common.Context, logger common.Logger) *directory {
	return &directory{
		logger: logger.Fmt("Directory"),
		Core:   newStorage(ctx, logger),
	}
}

func (d *directory) Close() (ret error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.closed {
		return dirClosedError
	}

	return d.Core.Close()
}

func (d *directory) OnChange(fn func([]event)) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.closed {
		return dirClosedError
	}

	d.Core.Listen(func(batch []item) {
		fn(dirItemsToEvents(batch))
	})

	return nil
}

func (d *directory) OnJoin(fn func(uuid.UUID, int)) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.closed {
		return dirClosedError
	}

	d.Core.ListenRoster(func(id uuid.UUID, ver int, status bool) {
		if status {
			fn(id, ver)
		}
	})

	return nil
}

func (d *directory) OnEviction(fn func(uuid.UUID, int)) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.closed {
		return dirClosedError
	}

	d.Core.ListenRoster(func(id uuid.UUID, ver int, status bool) {
		if !status {
			fn(id, ver)
		}
	})
	return nil
}

func (d *directory) OnFailure(fn func(uuid.UUID, int)) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.closed {
		return dirClosedError
	}

	d.Core.ListenHealth(func(id uuid.UUID, ver int, status bool) {
		if !status {
			fn(id, ver)
		}
	})
	return nil
}

func (d *directory) Apply(events []event) (ret []bool, err error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.closed {
		return nil, dirClosedError
	}

	ret = make([]bool, 0, len(events))
	d.Core.Update(func(u *update) {
		for _, e := range events {
			ret = append(ret, e.Apply(u))
		}
	})
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

func (d *directory) Add(m member) (err error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.closed {
		return dirClosedError
	}

	d.Core.Update(func(u *update) {
		if !m.Healthy {
			err = errors.Errorf("Cannot join unhealthy member [%v]", m)
			return
		}

		if !u.Join(m.id, m.version) {
			err = errors.Errorf("Member alread joined [%v]", m)
			return
		}

		u.Put(m.id, m.version, memberHostAttr, m.Host, m.version)
		u.Put(m.id, m.version, memberPortAttr, m.Port, m.version)
	})
	return
}

func (d *directory) Evict(m Member) (err error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.closed {
		return dirClosedError
	}

	d.Core.Update(func(u *update) {
		if !u.Evict(m.Id(), m.Version()) {
			err = errors.Errorf("Member already evicted [%v]")
			return
		}
	})
	return
}

func (d *directory) Fail(m Member) (err error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.closed {
		return dirClosedError
	}

	d.Core.Update(func(u *update) {
		if !u.Del(m.Id(), m.Version(), memberHealthAttr, m.Version()) {
			err = errors.Errorf("Unable to fail member [%v]", m)
		}
	})
	return
}

func (d *directory) Health(id uuid.UUID) (h health, ok bool) {
	d.Core.View(func(v *view) {
		h, ok = v.Health[id]
	})
	return
}

func (d *directory) Membership(id uuid.UUID) (m membership, ok bool) {
	d.Core.View(func(v *view) {
		m, ok = v.Roster[id]
	})
	return
}

func (d *directory) IsHealthy(id uuid.UUID) (ret bool) {
	d.Core.View(func(v *view) {
		ret = v.Health[id].Healthy
	})
	return
}

func (d *directory) IsActive(id uuid.UUID) (ret bool) {
	d.Core.View(func(v *view) {
		ret = v.Roster[id].Active
	})
	return
}

func (d *directory) AllHealthy() (ret []member) {
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

func (d *directory) AllFailed() (ret []member) {
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

func (d *directory) AllActive() (ret []member) {
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

func (d *directory) RecentlyEvicted() (ret []member) {
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

func (d *directory) Get(id uuid.UUID) (ret member, ok bool) {
	d.Core.View(func(u *view) {
		ret, ok = dirGetActiveMember(u, id)
	})
	return
}

func (d *directory) Search(filter func(uuid.UUID, string, string) bool) (ret []member) {
	ret = []member{}
	d.Core.View(func(v *view) {
		ids := make(map[uuid.UUID]struct{})

		v.ScanActive(func(s amoeba.Scan, i item) {
			if filter(i.MemId, i.Key, i.Val) {
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
			if filter(i.MemId, i.Key, i.Val) {
				defer s.Stop()
				ret, ok = dirGetActiveMember(v, i.MemId)
			}
		})
	})
	return
}

// Retrieves an active member from the directory.  To be active,
// means that no eviction has been seen for this memeber.
func dirGetActiveMember(v *view, id uuid.UUID) (member, bool) {
	if m, ok := dirGetMember(v, id); ok {
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
		id:      id,
		Host:    hostItem.Val,
		Port:    portItem.Val,
		Healthy: health.Healthy,
		Active:  mem.Active,
		version: hostItem.Ver}, true
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

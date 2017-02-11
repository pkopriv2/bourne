package convoy

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
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
	return dir.Listen()
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

func eventParser(r scribe.Reader) (interface{}, error) {
	return readItem(r)
}

type directory struct {
	Core *storage
}

func newDirectory(ctx common.Context) *directory {
	return &directory{
		Core: newStorage(ctx),
	}
}

func (d *directory) Close() (ret error) {
	return d.Core.Close()
}

func (d *directory) Listen() <-chan []event {
	ch := d.Core.Listen()
	ret := make(chan []event)
	go func() {
		for batch := range ch {
			ret <- dirItemsToEvents(batch)
		}
		close(ret)
	}()
	return ret
}

func (d *directory) Joins() <-chan membership {
	ch := streamMemberships(d.Core.Listen())
	ret := make(chan membership)
	go func() {
		for m := range ch {
			if m.Active {
				ret <- m
			}
		}
		close(ret)
	}()
	return ret
}

func (d *directory) Evictions() <-chan membership {
	ch := streamMemberships(d.Core.Listen())
	ret := make(chan membership)
	go func() {
		for m := range ch {
			if !m.Active {
				ret <- m
			}
		}
		close(ret)
	}()
	return ret
}

func (d *directory) Failures() <-chan health {
	ch := streamHealth(d.Core.Listen())
	ret := make(chan health)
	go func() {
		for h := range ch {
			if !h.Healthy {
				ret <- h
			}
		}
		close(ret)
	}()
	return ret
}

func (d *directory) Apply(events []event) (ret []bool, err error) {
	ret = make([]bool, 0, len(events))
	err = d.Core.Update(func(u *update) error {
		for _, e := range events {
			ret = append(ret, e.Apply(u))
		}
		return nil
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

func (d *directory) Add(m member) error {
	return d.Core.Update(func(u *update) error {
		if !m.healthy {
			return errors.Errorf("Cannot join unhealthy member [%v]", m)
		}

		if !u.Join(m.id, m.version) {
			return errors.Errorf("Member alread joined [%v]", m)
		}

		u.Put(m.id, m.version, memberHostAttr, m.host, m.version)
		u.Put(m.id, m.version, memberPortAttr, m.port, m.version)
		return nil
	})
}

func (d *directory) Evict(m Member) error {
	return d.Core.Update(func(u *update) error {
		if !u.Evict(m.Id(), m.Version()) {
			return errors.Wrapf(EvictedError, "Error evicting member [%v]", m)
		}
		return nil
	})
}

func (d *directory) Fail(m Member) error {
	d.Core.logger.Error("Failing member [%v]", m)
	return d.Core.Update(func(u *update) error {
		if !u.Del(m.Id(), m.Version(), memberHealthAttr, m.Version()) {
			return errors.Wrapf(FailedError, "Error failing member [%v]", m)
		}
		return nil
	})
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
		if m.active {
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
		host:    hostItem.Val,
		port:    portItem.Val,
		healthy: health.Healthy,
		active:  mem.Active,
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

func toMembers(arr []member) []Member {
	ret := make([]Member, 0, len(arr))
	for _, m := range arr {
		ret = append(ret, m)
	}

	return ret
}

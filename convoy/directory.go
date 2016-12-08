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
	for _, fn := range d.Listeners() {
		fn(batch)
	}
}

func (d *directory) ApplyAll(events []event) (ret []bool) {
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

func (d *directory) Join(m *member) (err error) {
	d.Core.Update(func(u *update) {
		if !u.Enable(m.Id, m.Version) {
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
	return
}

func (d *directory) Leave(m *member) (err error) {
	d.Core.Update(func(u *update) {
		if !u.Disable(m.Id, m.Version) {
			err = errors.Errorf("Member already joined [%v]", m.Id)
			return
		}
	})
	return
}

func (d *directory) Fail(m *member) (err error) {
	d.Core.Update(func(u *update) {
		if !u.Del(m.Id, m.Version, memberHealthAttr, m.Version) {
			err = errors.Errorf("Unable to fail member [%v]", m)
		}
	})
	return
}

// Retrieves a member from the directory
func dirGetMember(v *view, id uuid.UUID) *member {
	hostItem, found := v.GetLive(id, memberHostAttr)
	if !found {
		return nil
	}

	portItem, found := v.GetLive(id, memberPortAttr)
	if !found {
		return nil
	}

	statItem, found := v.GetLive(id, memberHealthAttr)
	return &member{
		Id:      id,
		Host:    hostItem.Val,
		Port:    portItem.Val,
		Healthy: !statItem.Del,
		Version: hostItem.Ver}
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

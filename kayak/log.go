package kayak

import (
	"fmt"

	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
)

// The event log maintains a set of events sorted (ascending) by
// insertion time.
type eventLogListener struct {
	log    *eventLog
	ch     chan eventLogItem
	closed chan struct{}
	closer chan struct{}
}

func newEventLogListener(log *eventLog) *eventLogListener {
	return &eventLogListener{log, make(chan eventLogItem, 1024), make(chan struct{}), make(chan struct{}, 1)}
}

func (l *eventLogListener) Closed() <-chan struct{} {
	return l.closed
}

func (l *eventLogListener) Items() <-chan eventLogItem {
	return l.ch
}

func (l *eventLogListener) Close() error {
	select {
	case <-l.closed:
		return ClosedError
	case l.closer <- struct{}{}:
	}

	l.log.subs.Remove(l)
	close(l.closed)
	return nil
}

type eventLogItem struct {
	index int
	term  int
	event Event
}

// The event log implementation.  The event log
type eventLog struct {
	data amoeba.Index

	// index of last item inserted. (initialized to -1)
	head int

	// index of last item committed.(initialized to -1)
	commit int

	// A channel of committed events.
	subs concurrent.Map

	// closing utilities.
	closed chan struct{}

	// closing lock.  (a buffered channel of 1 entry.)
	closer chan struct{}
}

func newEventLog(ctx common.Context) *eventLog {
	return &eventLog{amoeba.NewBTreeIndex(32), -1, -1, concurrent.NewMap(), make(chan struct{}), make(chan struct{}, 1)}
}

func (d *eventLog) Close() error {
	select {
	case <-d.closed:
		return ClosedError
	case d.closer <- struct{}{}:
	}

	for _, l := range d.listeners() {
		l.Close()
	}

	close(d.closed)
	return nil
}

func (c *eventLog) ensureOpen() error {
	select {
	case <-c.closed:
		return ClosedError
	default:
		return nil
	}
}

func (c *eventLog) listeners() (ret []*eventLogListener) {
	all := c.subs.All()
	ret = make([]*eventLogListener, 0, len(all))
	for k, _ := range all {
		ret = append(ret, k.(*eventLogListener))
	}
	return
}

func (c *eventLog) Listen() (*eventLogListener, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}

	ret := newEventLogListener(c)
	c.subs.Put(ret, struct{}{})
	return ret, nil
}

func (d *eventLog) Head() (pos int) {
	d.data.Update(func(u amoeba.Update) {
		pos = d.head
	})
	return
}

func (d *eventLog) Committed() (pos int) {
	d.data.Update(func(u amoeba.Update) {
		pos = d.commit
	})
	return
}

func (d *eventLog) snapshot(u amoeba.View) (int, int, int) {
	if d.head < 0 {
		return -1, -1, -1
	}

	item, _ := d.get(u, d.head)
	return item.index, item.term, d.commit
}

func (d *eventLog) Snapshot() (index int, term int, commit int) {
	d.data.Read(func(u amoeba.View) {
		index, term, commit = d.snapshot(u)
	})
	return
}

func (d *eventLog) get(u amoeba.View, index int) (item eventLogItem, ok bool) {
	val := u.Get(amoeba.IntKey(index))
	if val == nil {
		return
	}

	return val.(eventLogItem), true
}

func (d *eventLog) Commit(pos int) {
	if pos < 0 {
		return // -1 is normal
	}

	d.data.Update(func(u amoeba.Update) {
		if pos > d.head {
			panic(fmt.Sprintf("Invalid commit [%v/%v]", pos, d.head))
		}

		if pos <= d.commit {
			return // supports out of order commits.
		}

		committed := []eventLogItem{}
		if pos-d.commit > 0 {
			committed = eventLogScan(u, d.commit+1, pos-d.commit)
		}

		d.commit = pos

		listeners := d.listeners()
		for _, e := range committed {
			for _, l := range listeners {
				select {
				case <-d.closed:
					return
				case <-l.closed:
					return
				case l.ch <- e:
				}
			}
		}
	})
	return
}

func (d *eventLog) Get(index int) (item eventLogItem, ok bool) {
	d.data.Read(func(u amoeba.View) {
		item, ok = d.get(u, index)
	})
	return
}

func (d *eventLog) Scan(start int, num int) (batch []eventLogItem) {
	if num < 0 {
		panic("Invalid number of items to scan")
	}

	d.data.Read(func(u amoeba.View) {
		batch = eventLogScan(u, start, num)
	})
	return
}

func (d *eventLog) Append(batch []Event, term int) (index int) {
	if len(batch) == 0 {
		return
	}

	d.data.Update(func(u amoeba.Update) {
		index, _, _ = d.snapshot(u)
		for _, e := range batch {
			index++
			u.Put(amoeba.IntKey(index), eventLogItem{index, term, e})
		}

		d.head = index
	})
	return
}

func (d *eventLog) Insert(batch []Event, index int, term int) {
	if len(batch) == 0 {
		return
	}

	d.data.Update(func(u amoeba.Update) {
		for _, e := range batch {
			u.Put(amoeba.IntKey(index), eventLogItem{index, term, e})
			if index > d.head {
				d.head = index
			}

			index++
		}
	})
}

func eventLogScan(u amoeba.View, start int, num int) []eventLogItem {
	batch := make([]eventLogItem, 0, num)
	u.ScanFrom(amoeba.IntKey(start), func(s amoeba.Scan, k amoeba.Key, i interface{}) {
		if num == 0 {
			s.Stop()
			return
		}

		batch = append(batch, i.(eventLogItem))
		num--
	})

	return batch
}

func eventLogExtractEvents(items []eventLogItem) []Event {
	ret := make([]Event, 0, len(items))
	for _, i := range items {
		ret = append(ret, i.event)
	}
	return ret
}

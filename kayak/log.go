package kayak

import (
	"fmt"

	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
)

// The event log maintains a set of events sorted (ascending) by
// insertion time.

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
	ch chan Event

	// closing utilities.
	closed chan struct{}

	// closing lock.  (a buffered channel of 1 entry.)
	closer chan struct{}
}

func newEventLog(ctx common.Context) *eventLog {
	return &eventLog{amoeba.NewBTreeIndex(32), -1, -1, make(chan Event, 1024), make(chan struct{}), make(chan struct{}, 1)}
}

func (d *eventLog) Close() error {
	select {
	case <-d.closed:
		return ClosedError
	case d.closer <- struct{}{}:
	}

	close(d.ch)
	close(d.closed)
	return nil
}

func (d *eventLog) Commits() <-chan Event {
	return d.ch
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

		committed := []Event{}
		if pos-d.commit > 0 {
			committed = eventLogScan(u, d.commit+1, pos-d.commit)
		}

		d.commit = pos
		for _, e := range committed {
			select {
			case <-d.closed:
				return
			case d.ch <- e:
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

func (d *eventLog) Scan(start int, num int) (batch []Event) {
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

func eventLogScan(u amoeba.View, start int, num int) []Event {
	batch := make([]Event, 0, num)
	u.ScanFrom(amoeba.IntKey(start), func(s amoeba.Scan, k amoeba.Key, i interface{}) {
		if num == 0 {
			s.Stop()
			return
		}

		batch = append(batch, i.(eventLogItem).event)
		num--
	})

	return batch
}

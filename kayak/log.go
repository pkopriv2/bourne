package kayak

import (
	"fmt"

	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
)

// The event log maintains a set of events sorted (descending) by the
// number of remainaing attempts to be shared.

type eventLogItem struct {
	index int
	term  int
	event event
}

// The event log implementation.  The event log
type eventLog struct {
	data amoeba.Index

	// index of last item inserted. (initialized to -1)
	head int

	// index of last item committed.(initialized to -1)
	commit int
}

func newEventLog(ctx common.Context) *eventLog {
	return &eventLog{amoeba.NewBTreeIndex(32), -1, -1}
}

// returns and removes a batch of entries from the log.  nil if none.
func (d *eventLog) Commit(pos int) {
	if pos < 0  {
		panic("negative commit")
	}

	if pos > d.head {
		panic(fmt.Sprintf("Invalid commit [%v/%v]", pos, d.head))
	}

	if pos < d.commit {
		panic(fmt.Sprintf("Invalid commit [%v/%v].  Commit must move forward", pos, d.commit))
	}

	d.data.Update(func(u amoeba.Update) {
		d.commit = pos
	})
	return
}

func (d *eventLog) Committed() (pos int) {
	d.data.Update(func(u amoeba.Update) {
		pos = d.commit
	})
	return
}

func (d *eventLog) Head() (pos int) {
	d.data.Update(func(u amoeba.Update) {
		pos = d.head
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

func (d *eventLog) Get(index int) (item eventLogItem, ok bool) {
	d.data.Read(func(u amoeba.View) {
		item, ok = d.get(u, index)
	})
	return
}

func (d *eventLog) Scan(start int, num int) (batch []event) {
	if num < 0 {
		panic("Invalid number of items to scan")
	}

	d.data.Read(func(u amoeba.View) {
		batch = make([]event, 0, num)
		u.ScanFrom(amoeba.IntKey(start), func(s amoeba.Scan, k amoeba.Key, i interface{}) {
			if num == 0 {
				s.Stop()
				return
			}

			batch = append(batch, i.(eventLogItem).event)
			num--
		})
	})
	return
}

func (d *eventLog) Append(batch []event, term int) (index int) {
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

func (d *eventLog) Insert(batch []event, index int, term int) {
	if len(batch) == 0 || index < 1 {
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

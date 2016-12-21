package kayak

import (
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
	data   amoeba.Index
	commit int
	read   int
}

func newViewLog(ctx common.Context) *eventLog {
	return &eventLog{amoeba.NewBTreeIndex(32), 0, 0}
}

// returns and removes a batch of entries from the log.  nil if none.
func (d *eventLog) Commit(pos int) {
	d.data.Update(func(u amoeba.Update) {
		d.commit = pos
	})
	return
}

func (d *eventLog) snapshot(u amoeba.View) (int, int, int) {
		items := eventLogPeek(u, 1)
		if len(items) == 0 {
			return 0,0,0
		}

		return items[0].index, items[0].term, d.commit
}

func (d *eventLog) Snapshot() (index int, term int, commit int) {
	d.data.Read(func(u amoeba.View) {
		index, term, commit = d.snapshot(u)
	})
	return
}

func (d *eventLog) Get(index int) (term int, e event) {
	d.data.Read(func(u amoeba.View) {
		val := u.Get(amoeba.IntDescKey(index))
		if val == nil {
			return
		}

		item := val.(eventLogItem)
		term = item.term
		e = item.event
	})
	return
}

func (d *eventLog) Read(size int) (batch []event) {
	batch = []event{}
	d.data.Update(func(u amoeba.Update) {
		tmp, next := eventLogPop(u, d.read, d.commit, size)
		defer func() { d.read = next }()
		batch = tmp
	})
	return
}

func (d *eventLog) Append(batch []event, term int) {
	if len(batch) == 0 {
		return
	}

	d.data.Update(func(u amoeba.Update) {
		index, _, _ := d.snapshot(u)
		for i, e := range batch {
			u.Put(amoeba.IntDescKey(index), eventLogItem{index + i, term, e})
			index++
		}
	})
}

func (d *eventLog) Insert(batch []event, offset int, term int) {
	if len(batch) == 0 || offset < 1 {
		return
	}

	d.data.Update(func(u amoeba.Update) {
		for i, e := range batch {
			u.Put(amoeba.IntDescKey(offset), eventLogItem{offset + i, term, e})
			offset++
		}
	})
}

// this method is only here for parity.

func eventLogScan(data amoeba.View, fn func(amoeba.Scan, int, int, event)) {
	data.Scan(func(s amoeba.Scan, k amoeba.Key, i interface{}) {
		item := i.(eventLogItem)
		key := k.(amoeba.IntDescKey)
		fn(s, int(key), item.term, item.event)
	})
}

func eventLogPeek(data amoeba.View, num int) []eventLogItem {
	batch := make([]eventLogItem, 0, 128)
	eventLogScan(data, func(s amoeba.Scan, index int, term int, e event) {
		defer func() { num-- }()
		if num == 0 {
			s.Stop()
			return
		}

		batch = append(batch, eventLogItem{index, term, e})
	})
	return batch
}

func eventLogPop(data amoeba.Update, start int, horizon int, num int) ([]event, int) {
	read := make([]event, 0, 128)
	dead := make([]int, 0, 128)

	// We need to ass
	next := start
	eventLogScan(data, func(s amoeba.Scan, index int, term int, e event) {
		defer func() { num-- }()
		if num == 0 {
			s.Stop()
			return
		}

		if index > horizon {
			s.Stop()
			return
		}

		if index > next {
			s.Stop()
			return
		}

		dead = append(dead, index) // handles late/duplicate retrievals
		if index < next {
			return
		}

		read = append(read, e)
		next++
	})

	for _, k := range dead {
		data.Del(amoeba.IntDescKey(k))
	}

	return read, next
}

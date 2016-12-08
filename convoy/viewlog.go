package convoy

import (
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

// The event log maintains a set of events sorted (descending) by the
// number of remainaing attempts to be shared.

// the index key
type viewLogKey struct {
	Id        uuid.UUID
	Remaining int
}

func (e viewLogKey) Update(weight int) viewLogKey {
	return viewLogKey{e.Id, weight}
}

func (e viewLogKey) Compare(other amoeba.Sortable) int {
	o := other.(viewLogKey)
	if e.Remaining != o.Remaining {
		return o.Remaining - e.Remaining // Reverse cnt order.
	}

	return amoeba.CompareUUIDs(e.Id, o.Id)
}

// the index key,value pair
type viewLogEntry struct {
	Key   viewLogKey
	Event event
}

// this method is only here for parity.
func viewLogUnpackAmoebaKey(k amoeba.Key) viewLogKey {
	return k.(viewLogKey)
}

func viewLogUnpackAmoebaItem(item amoeba.Item) event {
	if item == nil {
		return nil
	}

	raw := item.Val()
	if raw == nil {
		return nil
	}

	return raw.(event)
}

func viewLogScan(data amoeba.View, fn func(amoeba.Scan, viewLogKey, event)) {
	data.Scan(func(s amoeba.Scan, k amoeba.Key, i amoeba.Item) {
		evt := viewLogUnpackAmoebaItem(i)
		if evt == nil {
			return // shouldn't be possible...but guarding anyway.
		}

		fn(s, viewLogUnpackAmoebaKey(k), evt)
	})
}

func viewLogPeek(data amoeba.View, num int) (batch []event) {
	batch = make([]event, 0, 128)
	viewLogScan(data, func(s amoeba.Scan, k viewLogKey, e event) {
		defer func() { num-- }()
		if num == 0 {
			s.Stop()
			return
		}

		batch = append(batch, e)
	})
	return
}

func viewLogPop(data amoeba.Update, num int) (batch []event) {
	batch = make([]event, 0, 128)
	viewed := make([]viewLogKey, 0, 128)

	viewLogScan(data, func(s amoeba.Scan, k viewLogKey, e event) {
		defer func() { num-- }()
		if num == 0 {
			s.Stop()
			return
		}

		batch = append(batch, e)
		viewed = append(viewed, k)
	})

	for i, v := range viewed {
		data.DelNow(v)
		if v.Remaining <= 1 {
			continue
		}

		data.Put(viewLogKey{v.Id, v.Remaining - 1}, batch[i], 0)
	}

	return
}

// The event log implementation.  The event log
type viewLog struct {
	Data amoeba.Index
}

func newViewLog(ctx common.Context) *viewLog {
	return &viewLog{amoeba.NewIndexer(ctx)}
}

func (c *viewLog) Close() error {
	return c.Data.Close()
}

// returns, but does not remove a batch of entries from the log.  nil if none.
func (d *viewLog) Peek(size int) (batch []event) {
	batch = []event{}
	d.Data.Read(func(v amoeba.View) {
		batch = viewLogPeek(v, size)
	})
	return
}

// returns and removes a batch of entries from the log.  nil if none.
func (d *viewLog) Pop(size int) (batch []event) {
	batch = []event{}
	d.Data.Update(func(u amoeba.Update) {
		batch = viewLogPop(u, size)
	})
	return
}

func (d *viewLog) Push(batch []event, n int) {
	if len(batch) == 0 || n < 1 {
		return
	}

	d.Data.Update(func(u amoeba.Update) {
		for _, e := range batch {
			u.Put(viewLogKey{uuid.NewV1(), n}, e, 0) // NOTE: using V1 uuid so we don't exhaust entropy.
		}
	})
}

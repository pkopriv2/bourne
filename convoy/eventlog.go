package convoy

import (
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

// The event log maintains a set of events sorted (descending) by the
// number of remainaing attempts to be shared.

// the index key
type eventLogKey struct {
	Id     uuid.UUID
	Weight int
}

func (e eventLogKey) Update(weight int) eventLogKey {
	return eventLogKey{e.Id, weight}
}

func (e eventLogKey) Compare(other amoeba.Sortable) int {
	o := other.(eventLogKey)
	if e.Weight != o.Weight {
		return o.Weight - e.Weight // Reverse cnt order.
	}

	return amoeba.CompareUUIDs(e.Id, o.Id)
}

// the index key,value pair
type eventLogEntry struct {
	Key   eventLogKey
	Event event
}

// this method is only here for parity.
func eventLogUnpackAmoebaKey(k amoeba.Key) eventLogKey {
	return k.(eventLogKey)
}

func eventLogUnpackAmoebaItem(item amoeba.Item) event {
	if item == nil {
		return nil
	}

	raw := item.Val()
	if raw == nil {
		return nil
	}

	return raw.(event)
}

func eventLogGetEvent(data amoeba.View, key eventLogKey) event {
	return eventLogUnpackAmoebaItem(data.Get(key))
}

func eventLogPutEvent(data amoeba.Update, key eventLogKey, e event) bool {
	return data.Put(key, e, 0) // shouldn't ever collide.
}

func eventLogDelEvent(data amoeba.Update, key eventLogKey) {
	data.DelNow(key)
}

func eventLogScan(data amoeba.View, fn func(*amoeba.Scan, eventLogKey, event)) {
	data.Scan(func(s *amoeba.Scan, k amoeba.Key, i amoeba.Item) {
		evt := eventLogUnpackAmoebaItem(i)
		if evt == nil {
			return // shouldn't be possible...but guarding anyway.
		}

		fn(s, eventLogUnpackAmoebaKey(k), evt)
	})
}

// A couple very simple low level view/update abstractions
type evtLogUpdate struct {
	Data amoeba.Update
}

func (u *evtLogUpdate) Add(key eventLogKey, e event) {
	eventLogPutEvent(u.Data, key, e)
}

func (u *evtLogUpdate) Push(batch []eventLogEntry) {
	for _, e := range batch {
		if e.Key.Weight >= 1 {
			eventLogPutEvent(u.Data, e.Key, e.Event)
		}
	}
}

func (u *evtLogUpdate) Peek(size int) (batch []eventLogEntry) {
	batch = make([]eventLogEntry, 0, size)
	eventLogScan(u.Data, func(s *amoeba.Scan, k eventLogKey, e event) {
		defer func() { size-- }()
		if size == 0 {
			s.Stop()
			return
		}

		batch = append(batch, eventLogEntry{k, e})
	})

	return
}

func (u *evtLogUpdate) Pop(size int) (batch []eventLogEntry) {
	batch = u.Peek(size)
	for _, entry := range batch {
		eventLogDelEvent(u.Data, entry.Key)
	}
	return
}

// The event log implementation.  The event log
type eventLog struct {
	Data amoeba.Indexer
}

func newEventLog(ctx common.Context) *eventLog {
	return &eventLog{amoeba.NewIndexer(ctx)}
}

func (c *eventLog) Close() error {
	return c.Data.Close()
}

func (d *eventLog) Update(fn func(*evtLogUpdate)) {
	d.Data.Update(func(data amoeba.Update) {
		fn(&evtLogUpdate{Data: data})
	})
}

// returns, but does not remove a batch of entries from the log.  nil if none.
func (d *eventLog) Peek(size int) (batch []eventLogEntry) {
	batch = []eventLogEntry{}
	d.Update(func(u *evtLogUpdate) {
		batch = u.Peek(size)
	})
	return
}

// returns, but does not remove a batch of entries from the log.  nil if none.
func (d *eventLog) PeekRaw(size int) (batch []event) {
	d.Update(func(u *evtLogUpdate) {
		batch = eventLogExtractEvents(u.Peek(size))
	})
	return
}

// returns and removes a batch of entries from the log.  nil if none.
func (d *eventLog) Pop(size int) (batch []eventLogEntry) {
	batch = []eventLogEntry{}
	d.Update(func(u *evtLogUpdate) {
		batch = u.Pop(size)
	})
	return
}

func (d *eventLog) Return(batch []eventLogEntry) {
	d.Update(func(u *evtLogUpdate) {
		u.Push(batch)
	})
	return
}

func (d *eventLog) Add(batch []event, n int) {
	if len(batch) == 0 || n < 1 {
		return
	}

	d.Update(func(u *evtLogUpdate) {
		for _, e := range batch {
			u.Add(eventLogKey{uuid.NewV1(), n}, e) // NOTE: using V1 uuid so we don't exhaust entropy.
		}
	})
}

func eventLogDecBatch(batch []eventLogEntry) (events []eventLogEntry) {
	events = make([]eventLogEntry, 0, len(batch))
	for _, entry := range batch {
		events = append(events, eventLogDec(entry))
	}
	return
}

func eventLogDec(e eventLogEntry) eventLogEntry {
	return eventLogEntry{eventLogKey{e.Key.Id, e.Key.Weight - 1}, e.Event}
}

func eventLogExtractEvents(batch []eventLogEntry) (events []event) {
	events = make([]event, 0, len(batch))
	for _, entry := range batch {
		events = append(events, entry.Event)
	}
	return
}

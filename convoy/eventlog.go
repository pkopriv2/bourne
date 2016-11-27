package convoy

import (
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

// the index value
type eventLogKey struct {
	id  uuid.UUID
	cnt int
}

func (e eventLogKey) Compare(other amoeba.Sortable) int {
	o := other.(eventLogKey)
	if e.cnt != o.cnt {
		return o.cnt - e.cnt // Reverse cnt order.
	}

	return amoeba.CompareUUIDs(e.id, o.id)
}

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

func (u *evtLogUpdate) SucceedBatch(batch []eventLogEntry) {
	for _, e := range batch {
		if e.Key.cnt > 1 {
			eventLogPutEvent(u.Data, eventLogKey{e.Key.id, e.Key.cnt - 1}, e.Event)
		}
	}
}

func (u *evtLogUpdate) FailBatch(batch []eventLogEntry) {
	for _, e := range batch {
		eventLogPutEvent(u.Data, e.Key, e.Event)
	}
}

func (u *evtLogUpdate) NextBatch(size int) (batch []eventLogEntry) {
	batch = make([]eventLogEntry, 0, size)
	eventLogScan(u.Data, func(s *amoeba.Scan, k eventLogKey, e event) {
		defer func() { size-- }()
		if size == 0 {
			s.Stop()
			return
		}

		batch = append(batch, eventLogEntry{k, e})
	})

	// I find it best to actually remove the items from the queue.  This
	// way, we're not limiting the number of actors who can process.
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

func (d *eventLog) PushBatch(batch []event, n int) {
	if len(batch) == 0 || n < 1 {
		return
	}

	d.Update(func(u *evtLogUpdate) {
		for _, e := range batch {
			u.Add(eventLogKey{uuid.NewV1(), n}, e) // NOTE: using V1 uuid so we don't exhaust entropy.
		}
	})
}

func (d *eventLog) Push(e event, successes int) {
	d.PushBatch([]event{e}, successes)
}

func (d *eventLog) nextBatch(size int) (batch []eventLogEntry) {
	batch = []eventLogEntry{}
	d.Update(func(u *evtLogUpdate) {
		batch = u.NextBatch(size)
	})
	return
}

func (d *eventLog) Process(fn func([]event) error) (err error) {
	batch := d.nextBatch(256)

	// IMPORTANT: Make sure function invocation never involves lock!
	err = fn(eventLogExtractEvents(batch))

	d.Update(func(u *evtLogUpdate) {
		if err != nil {
			u.FailBatch(batch)
		} else {
			u.SucceedBatch(batch)
		}
	})

	return
}

func eventLogExtractEvents(batch []eventLogEntry) (events []event) {
	events = make([]event, 0, len(batch))
	for _, entry := range batch {
		{
			events = append(events, entry.Event)
		}
	}
	return
}

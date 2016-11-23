package convoy

import (
	"github.com/pkopriv2/bourne/amoeba"
	uuid "github.com/satori/go.uuid"
)

// the index value
type evtKey struct {
	id  uuid.UUID
	cnt int
}

func (e evtKey) Compare(other amoeba.Sortable) int {
	o := other.(evtKey)
	if e.cnt != o.cnt {
		return o.cnt - e.cnt // Reverse cnt order.
	}

	return amoeba.CompareUUIDs(e.id, o.id)
}

type eventLogEntry struct {
	Key evtKey
	Event event
}

// this method is only here for parity.
func eventLogUnpackAmoebaKey(k amoeba.Key) evtKey {
	return k.(evtKey)
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

func eventLogGetEvent(data amoeba.View, key evtKey) event {
	return eventLogUnpackAmoebaItem(data.Get(key))
}

func eventLogPutEvent(data amoeba.Update, key evtKey, e event) bool {
	return data.Put(key, e, 0) // shouldn't ever collide.
}

func eventLogDelEvent(data amoeba.Update, key evtKey) {
	data.DelNow(key)
}

func eventLogScan(data amoeba.View, fn func(*amoeba.Scan, evtKey, event)) {
	data.Scan(func(s *amoeba.Scan, k amoeba.Key, i amoeba.Item) {
		evt := eventLogUnpackAmoebaItem(i)
		if evt == nil {
			return // shouldn't be possible...but guarding anyway.
		}

		fn(s, eventLogUnpackAmoebaKey(k), evt)
	})
}

func eventLogIncrement(data amoeba.Update, e eventLogEntry) {
	eventLogPutEvent(data, evtKey{e.Key.id, e.Key.cnt+1}, e.Event)
}

func eventLogIncrementBatch(data amoeba.Update, evts []eventLogEntry) {
	for _, e := range evts {
		eventLogIncrement(data, e)
	}
}


// A couple very simple low level view/update abstractions
type evtLogUpdate struct {
	Data amoeba.Update
}

func (u *evtLogUpdate) Del(k evtKey) {
	eventLogDelEvent(u.Data, k)
}

func (u *evtLogUpdate) Add(k evtKey, e event) {
	eventLogPutEvent(u.Data, k, e)
}

func (u *evtLogUpdate) NextBatch(size int) []eventLogEntry {
	ret := make([]eventLogEntry, 0, size)
	eventLogScan(u.Data, func(s *amoeba.Scan, k evtKey, e event) {
		defer func() {size--}()
		if size == 0 {
			s.Stop()
		}
		ret = append(ret, eventLogEntry{k, e})
	})

	for _, entry := range ret {
		eventLogDelEvent(u.Data, entry.Key)
	}

	return ret
}

// The event log implementation.  The event log is
// built on a bolt DB instance, so it is guaranteed
// both durable and thread-safe.
type eventLog struct {
	Data amoeba.Indexer
}

func (c *eventLog) Close() error {
	return c.Data.Close()
}

func (d *eventLog) Update(fn func(*evtLogUpdate)) {
	d.Data.Update(func(data amoeba.Update) {
		fn(&evtLogUpdate{Data: data})
	})
}

func (d *eventLog) Push(e event) {
	d.Update(func(u *evtLogUpdate) {
		u.Add(evtKey{uuid.NewV1(), 0}, e) // NOTE: using V1 uuid so we don't exhaust entropy.
	})
}

func (d *eventLog) ProcessBatch(size int, fn func([]event) bool) {
	var batch []eventLogEntry

	d.Update(func(u *evtLogUpdate) {
		batch = u.NextBatch(size)
	})

	if len(batch) == 0 {
		return
	}

	events := make([]event, 0, len(batch))
	for _, entry := range batch {
		events = append(events, entry.Event)
	}

	success := fn(events)

	d.Update(func(u *evtLogUpdate) {
		for _, e := range batch {
			if ! success {
				u.Add(e.Key, e.Event)
				continue
			}

			if e.Key.cnt > 0 {
				u.Add(evtKey{e.Key.id, e.Key.cnt-1}, e.Event)
			}
		}
	})
}

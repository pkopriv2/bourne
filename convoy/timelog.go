package convoy

import (
	"sync"
	"time"

	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

// Adds a listener to the change log and returns a buffered channel of changes.
// the channel is closed when the log is closed.
func timeLogListen(tl *timeLog) <-chan []event {
	ret := make(chan []event, 1024)
	tl.Listen(func(batch []event) {
		if batch == nil {
			close(ret)
			return
		}

		ret <- batch
	})
	return ret
}


const (
	ConfTimeLogHorizon = "convoy.timelog.horizon"
)

const (
	DefaultTimeLogHorizon = 30 * time.Minute
)

// Maintains a time sorted list of events up to some specified maximum ttl.
type timeLogKey struct {
	Id      uuid.UUID
	Created time.Time // assumed to be unique (time must be taken inside lock)
}

func (e timeLogKey) Compare(other amoeba.Sortable) int {
	o := other.(timeLogKey)
	if e.Created.Before(o.Created) {
		return 1
	}

	if e.Created.After(o.Created) {
		return -1
	}

	return amoeba.CompareUUIDs(e.Id, o.Id)
}

// this method is only here for parity.
func timeLogUnpackAmoebaKey(k amoeba.Key) timeLogKey {
	return k.(timeLogKey)
}

func timeLogUnpackAmoebaItem(item amoeba.Item) event {
	if item == nil {
		return nil
	}

	raw := item.Val()
	if raw == nil {
		return nil
	}

	return raw.(event)
}

func timeLogScanFrom(start timeLogKey, data amoeba.View, fn func(*amoeba.Scan, timeLogKey, event)) {
	data.ScanFrom(start, func(s *amoeba.Scan, k amoeba.Key, i amoeba.Item) {
		evt := timeLogUnpackAmoebaItem(i)
		if evt == nil {
			return // shouldn't be possible...but guarding anyway.
		}

		fn(s, timeLogUnpackAmoebaKey(k), evt)
	})
}

func timeLogScan(data amoeba.View, fn func(*amoeba.Scan, timeLogKey, event)) {
	data.Scan(func(s *amoeba.Scan, k amoeba.Key, i amoeba.Item) {
		evt := timeLogUnpackAmoebaItem(i)
		if evt == nil {
			return
		}

		fn(s, timeLogUnpackAmoebaKey(k), evt)
	})
}

// timeLog implementation.
type timeLog struct {
	Ctx      common.Context
	Data     amoeba.Indexer
	Dead     time.Duration
	Handlers []func([]event)
	Lock     sync.RWMutex
}

func newTimeLog(ctx common.Context) *timeLog {
	return &timeLog{
		Ctx:  ctx,
		Data: amoeba.NewIndexer(ctx),
		Dead: ctx.Config().OptionalDuration(ConfTimeLogHorizon, DefaultTimeLogHorizon)}
}

func (t *timeLog) Close() (ret error) {
	ret = t.Data.Close()
	t.broadcast(nil)
	return
}

func (t *timeLog) broadcast(batch []event) {
	for _, fn := range t.Listeners() {
		fn(batch)
	}
}

func (t *timeLog) Listeners() []func([]event) {
	t.Lock.RLock()
	defer t.Lock.RUnlock()
	ret := make([]func([]event), 0, len(t.Handlers))
	for _, fn := range t.Handlers {
		ret = append(ret, fn)
	}
	return ret
}

func (e *timeLog) Listen(fn func([]event)) {
	e.Lock.Lock()
	defer e.Lock.Unlock()
	e.Handlers = append(e.Handlers, fn)
}

func (t *timeLog) Peek(after time.Time) (ret []event) {
	ret = []event{}
	t.Data.Read(func(data amoeba.View) {
		horizon := data.Time().Add(-t.Dead)

		ret = make([]event, 0, 256)
		timeLogScan(data, func(s *amoeba.Scan, key timeLogKey, e event) {
			if key.Created.Before(after) || key.Created.Equal(after) || key.Created.Before(horizon) {
				defer s.Stop()
				return
			}

			ret = append(ret, e)
		})
	})
	return
}

func (t *timeLog) Push(batch []event) {
	t.Data.Update(func(data amoeba.Update) {
		for _, e := range batch {
			data.Put(timeLogKey{uuid.NewV1(), data.Time()}, e, 0)
		}

		t.gc(data)
	})

	t.broadcast(batch)
}

func (t *timeLog) gc(data amoeba.Update) {
	horizon := data.Time().Add(-t.Dead)

	dead := make([]timeLogKey, 0, 128)
	timeLogScanFrom(timeLogKey{amoeba.ZeroUUID, horizon}, data, func(s *amoeba.Scan, key timeLogKey, e event) {
		dead = append(dead, key)
	})

	for _, t := range dead {
		data.DelNow(t)
	}
}

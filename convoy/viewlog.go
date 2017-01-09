package convoy

import (
	"fmt"

	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

// The event log maintains a set of events sorted (descending) by the
// number of remainaing attempts to be shared.

// TODO: replace uuid with integer

// The event log implementation.  The event log
type viewLog struct {
	data amoeba.Index
}

func newViewLog(ctx common.Context) *viewLog {
	return &viewLog{amoeba.NewBTreeIndex(32)}
}

// returns, but does not remove a batch of entries from the log.  nil if none.
func (d *viewLog) Peek(size int) (batch []event) {
	batch = []event{}
	d.data.Read(func(v amoeba.View) {
		batch = viewLogPeek(v, size)
	})
	return
}

// returns and removes a batch of entries from the log.  nil if none.
func (d *viewLog) Pop(size int) (batch []event) {
	batch = []event{}
	d.data.Update(func(u amoeba.Update) {
		batch = viewLogPop(u, size)
	})
	return
}

func (d *viewLog) Push(batch []event, n int) {
	if len(batch) == 0 || n < 1 {
		return
	}

	d.data.Update(func(u amoeba.Update) {
		for _, e := range batch {
			u.Put(viewLogKey{uuid.NewV1(), n}, e) // NOTE: using V1 uuid so we don't exhaust entropy.
		}
	})
}

// the index key
type viewLogKey struct {
	Id        uuid.UUID
	Remaining int
}

func (e viewLogKey) Update(weight int) viewLogKey {
	return viewLogKey{e.Id, weight}
}

func (e viewLogKey) String() string {
	return fmt.Sprintf("/%v/%v", e.Id, e.Remaining)
}

func (e viewLogKey) Hash() string {
	return e.String()
}

func (e viewLogKey) Compare(other amoeba.Key) int {
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

func viewLogScan(data amoeba.View, fn func(amoeba.Scan, viewLogKey, event)) {
	data.Scan(func(s amoeba.Scan, k amoeba.Key, i interface{}) {
		fn(s, k.(viewLogKey), i.(event))
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

	for _, k := range viewed {
		data.Del(k)
	}

	for i, v := range viewed {
		if v.Remaining <= 1 {
			continue
		}

		data.Put(viewLogKey{v.Id, v.Remaining - 1}, batch[i])
	}

	return
}

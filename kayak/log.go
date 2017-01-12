package kayak

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
)

// The event log maintains a set of events sorted (ascending) by
// insertion time.

type eventLog struct {
	data amoeba.Index

	// index of last item inserted. (initialized to -1)
	appender *position

	// index of last item committed.(initialized to -1)
	committer *position

	// closing utilities.
	closed chan struct{}

	// closing lock.  (a buffered channel of 1 entry.)
	closer chan struct{}
}

func newEventLog(ctx common.Context) *eventLog {
	closed := make(chan struct{})
	return &eventLog{amoeba.NewBTreeIndex(32), newCommitter(closed), newCommitter(closed), closed, make(chan struct{}, 1)}
}

func (d *eventLog) Close() error {
	select {
	case <-d.closed:
		return ClosedError
	case d.closer <- struct{}{}:
	}

	d.committer.Notify()
	d.appender.Notify()
	close(d.closed)
	return nil
}

func (d *eventLog) ListenCommits(from int, buf int) (*commitListener, error) {
	select {
	case <-d.closed:
		return nil, ClosedError
	default:
	}

	return newCommitListener(d, from, buf), nil
}

func (d *eventLog) ListenCommitsLive(buf int) (*commitListener, error) {
	select {
	case <-d.closed:
		return nil, ClosedError
	default:
	}

	return newCommitListener(d, d.committer.Get(), buf), nil
}

func (d *eventLog) Head() (pos int) {
	return d.appender.Get()
}

func (d *eventLog) Committed() (pos int) {
	return d.committer.Get()
}

func (d *eventLog) snapshot(u amoeba.View) (int, int, int) {
	head := d.appender.Get()
	if head < 0 {
		return -1, -1, -1
	}

	item, _ := d.get(u, head)
	return item.Index, item.term, d.committer.Get()
}

func (d *eventLog) Snapshot() (index int, term int, commit int) {
	d.data.Read(func(u amoeba.View) {
		index, term, commit = d.snapshot(u)
	})
	return
}

func (d *eventLog) get(u amoeba.View, index int) (item LogItem, ok bool) {
	val := u.Get(amoeba.IntKey(index))
	if val == nil {
		return
	}

	return val.(LogItem), true
}

func (d *eventLog) Commit(pos int) {
	if pos < 0 {
		return // -1 is normal
	}

	d.committer.Update(func(cur int) int {
		return common.Min(pos, d.appender.Get())
	})
}

func (d *eventLog) Get(index int) (item LogItem, ok bool) {
	d.data.Read(func(u amoeba.View) {
		item, ok = d.get(u, index)
	})
	return
}

func (d *eventLog) Scan(start int, end int) (batch []LogItem) {
	if end < start {
		panic("Invalid number of items to scan")
	}

	d.data.Read(func(u amoeba.View) {
		batch = eventLogScan(u, start, end)
	})
	return
}

func (d *eventLog) Compact(snapshot []Event, head int) (err error) {
	d.data.Update(func(u amoeba.Update) {
		// get the
		raw, ok := d.get(u, head)
		if !ok {
			err = errors.Wrapf(StateError, "Unable to compact to [%v].  It doesn't exist!", head)
			return
		}

		minKey, _ := u.Min()
		min := int(minKey.(amoeba.IntKey))
		num := len(snapshot)

		// delete everything up to and including head
		for i:=min; i<head+1; i++ {
			u.Del(amoeba.IntKey(i))
		}

		// start inserting
		for i:=0; i<len(snapshot); i++ {
			u.Put(amoeba.IntKey(head-num+i), newEventLogItem(head-num+i, raw.term, snapshot[i]))
		}
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
			u.Put(amoeba.IntKey(index), LogItem{index, e, term})
		}

		d.appender.Set(index)
	})
	return
}

func (d *eventLog) Insert(batch []Event, index int, term int) {
	if len(batch) == 0 {
		return
	}

	d.data.Update(func(u amoeba.Update) {
		head := d.appender.Get()
		for _, e := range batch {
			u.Put(amoeba.IntKey(index), LogItem{index, e, term})
			if index > head {
				head = index
			}
			index++
		}

		d.appender.Set(head)
	})
}

type commitListener struct {
	log    *eventLog
	ch     chan LogItem
	closed chan struct{}
	closer chan struct{}
}

func newCommitListener(log *eventLog, from int, buf int) *commitListener {
	l := &commitListener{log, make(chan LogItem, buf), make(chan struct{}), make(chan struct{}, 1)}
	l.start(from)
	return l
}

func (l *commitListener) start(from int) {
	go func() {
		defer l.Close()

		for {
			next, err := l.log.committer.WaitForChange(from, l.closed)
			if err != nil {
				return
			}

			for _, i := range l.log.Scan(from, next+1) {
				select {
				case <-l.closed:
					return
				case <-l.log.closed:
					return
				case l.ch <- i:
				}
			}

			from = next + 1
		}
	}()
}

func (l *commitListener) Closed() <-chan struct{} {
	return l.closed
}

func (l *commitListener) Items() <-chan LogItem {
	return l.ch
}

func (l *commitListener) Close() error {
	select {
	case <-l.closed:
		return ClosedError
	case l.closer <- struct{}{}:
	}

	close(l.closed)
	return nil
}

type position struct {
	pos     int
	posLock *sync.Cond
	cancel  <-chan struct{}
}

func newCommitter(cancel <-chan struct{}) *position {
	return &position{-1, &sync.Cond{L: &sync.Mutex{}}, cancel}
}

func (c *position) WaitForChange(pos int, done <-chan struct{}) (commit int, err error) {
	c.posLock.L.Lock()
	defer c.posLock.L.Unlock()

	for commit = c.pos; commit <= pos; commit = c.pos {
		c.posLock.Wait()
		select {
		default:
			continue
		case <-done:
			return pos, CanceledError
		case <-c.cancel:
			return pos, CanceledError
		}
	}
	return
}

func (c *position) Notify() {
	c.posLock.Broadcast()
}

func (c *position) Update(fn func(int) int) {
	c.posLock.L.Lock()
	pos := fn(c.pos)
	if pos > c.pos {
		c.pos = pos
	}
	c.posLock.Broadcast()
	c.posLock.L.Unlock()
}

func (c *position) Set(pos int) {
	c.posLock.L.Lock()
	if pos > c.pos {
		c.pos = pos
	}

	c.posLock.Broadcast()
	c.posLock.L.Unlock()
}

func (c *position) Get() (pos int) {
	c.posLock.L.Lock()
	pos = c.pos
	c.posLock.L.Unlock()
	return
}

func eventLogScan(u amoeba.View, start int, end int) []LogItem {
	if start > end {
		panic("End must be greater than or equal to start")
	}

	batch := make([]LogItem, 0, end-start)
	u.ScanFrom(amoeba.IntKey(start), func(s amoeba.Scan, k amoeba.Key, i interface{}) {
		index := int(k.(amoeba.IntKey))
		if index >= end {
			s.Stop()
			return
		}

		batch = append(batch, i.(LogItem))
	})
	return batch
}

func eventLogExtractEvents(items []LogItem) []Event {
	ret := make([]Event, 0, len(items))
	for _, i := range items {
		ret = append(ret, i.Event)
	}
	return ret
}

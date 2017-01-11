package kayak

import (
	"fmt"
	"sync"

	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
)

// The event log maintains a set of events sorted (ascending) by
// insertion time.

type eventLog struct {
	data amoeba.Index

	// index of last item inserted. (initialized to -1)
	head int

	// index of last item committed.(initialized to -1)
	committer *committer

	// closing utilities.
	closed chan struct{}

	// closing lock.  (a buffered channel of 1 entry.)
	closer chan struct{}
}

func newEventLog(ctx common.Context) *eventLog {
	return &eventLog{amoeba.NewBTreeIndex(32), -1, newCommitter(), make(chan struct{}), make(chan struct{}, 1)}
}

func (d *eventLog) Close() error {
	select {
	case <-d.closed:
		return ClosedError
	case d.closer <- struct{}{}:
	}

	d.committer.Notify()
	close(d.closed)
	return nil
}

func (d *eventLog) Listen(from int, buf int) (*eventLogListener, error) {
	select {
	case <-d.closed:
		return nil, ClosedError
	default:
	}

	return newEventLogListener(d, from, buf), nil
}

func (d *eventLog) ListenLive(buf int) (*eventLogListener, error) {
	select {
	case <-d.closed:
		return nil, ClosedError
	default:
	}

	return newEventLogListener(d, d.committer.Get(), buf), nil
}

func (d *eventLog) Head() (pos int) {
	d.data.Update(func(u amoeba.Update) {
		pos = d.head
	})
	return
}

func (d *eventLog) Committed() (pos int) {
	return d.committer.Get()
}

func (d *eventLog) snapshot(u amoeba.View) (int, int, int) {
	if d.head < 0 {
		return -1, -1, -1
	}

	item, _ := d.get(u, d.head)
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

	d.committer.Update(func(val int) int {
		head := d.Head()
		if pos > head {
			panic(fmt.Sprintf("Invalid commit [%v/%v]", pos, head))
		}

		return common.Max(val, pos)
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
			u.Put(amoeba.IntKey(index), LogItem{index, e, term})
			if index > d.head {
				d.head = index
			}

			index++
		}
	})
}

type eventLogListener struct {
	log    *eventLog
	ch     chan LogItem
	closed chan struct{}
	closer chan struct{}
}

func newEventLogListener(log *eventLog, from int, buf int) *eventLogListener {
	l := &eventLogListener{log, make(chan LogItem, buf), make(chan struct{}), make(chan struct{}, 1)}
	l.start(from)
	return l
}

func (l *eventLogListener) start(from int) {
	go func() {
		defer l.Close()

		for {
			next, err := l.log.committer.WaitForChange(from, l.closed, l.log.closed)
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

			from = next+1
		}
	}()
}

func (l *eventLogListener) Closed() <-chan struct{} {
	return l.closed
}

func (l *eventLogListener) Items() <-chan LogItem {
	return l.ch
}

func (l *eventLogListener) Close() error {
	select {
	case <-l.closed:
		return ClosedError
	case l.closer <- struct{}{}:
	}

	close(l.closed)
	return nil
}

type committer struct {
	commit     int
	commitLock *sync.Cond
}

func newCommitter() *committer {
	return &committer{-1, &sync.Cond{L: &sync.Mutex{}}}
}

func (c *committer) WaitForChange(pos int, done1 <-chan struct{}, done2 <-chan struct{}) (commit int, err error) {
	c.commitLock.L.Lock()
	defer c.commitLock.L.Unlock()

	for commit = c.commit; commit <= pos; commit = c.commit {
		c.commitLock.Wait()
		select {
		default:
			continue
		case <-done1:
			return -1, ClosedError
		case <-done2:
			return -1, ClosedError
		}
	}
	return
}

func (c *committer) Notify() {
	c.commitLock.Broadcast()
}

func (c *committer) Update(fn func(int) int) {
	c.commitLock.L.Lock()
	c.commit = fn(c.commit)
	c.commitLock.L.Unlock()
	c.commitLock.Broadcast()
}

func (c *committer) Get() (pos int) {
	c.commitLock.L.Lock()
	pos = c.commit
	c.commitLock.L.Unlock()
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


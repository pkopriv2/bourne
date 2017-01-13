package kayak

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
)

// FIXME: compacting + listening is dangerous if the listener is significantly
// behind.  Generally, this is OKAY for us because the only time we'll be snapshotting
// will be AFTER the consumer state machine has safely received the values we'll be
// removing as part of the compaction.
//
// May need to more tightly coordinate compacting with listeners...or change
// the listening architecture to allow for more consistent listeners.

// The event log maintains a set of events sorted (ascending) by
// insertion time.

type segment struct {
	snapshot []Event

	//
	min *position

	//
	max *position

	//
	raw amoeba.Index
}

func newSegment(log *eventLog) *segment {
	return &segment{min: newPosition(-1, log.closed), max: newPosition(-1, log.closed), raw: amoeba.NewBTreeIndex(32)}
}

func (d *segment) Min() int {
	return d.min.Get()
}

func (d *segment) Head() int {
	return d.max.Get()
}

func (d *segment) get(u amoeba.View, index int) (item LogItem, ok bool) {
	val := u.Get(amoeba.IntKey(index))
	if val == nil {
		return
	}

	return val.(LogItem), true
}

func (d *segment) Last() (item LogItem) {
	d.raw.Read(func(u amoeba.View) {
		head := d.max.Get()
		if head < 0 {
			item = LogItem{Index: -1, term: -1}
			return
		}

		item, _ = d.get(u, head)
	})
	return
}

func (d *segment) Get(index int) (item LogItem, ok bool) {
	d.raw.Read(func(u amoeba.View) {
		item, ok = d.get(u, index)
	})
	return
}

func (d *segment) Scan(start int, end int) (batch []LogItem, err error) {
	if end < start {
		return nil, errors.Wrapf(StateError, "Invalid bounds [%v,%v]", start, end)
	}

	if start < d.Min() || end > d.Head() {
		return nil, errors.Wrapf(StateError, "Invalid bounds [%v,%v]. Exceeds bounds of log.", start, end)
	}

	d.raw.Read(func(u amoeba.View) {
		batch = make([]LogItem, 0, end-start)
		u.ScanFrom(amoeba.IntKey(start), func(s amoeba.Scan, k amoeba.Key, i interface{}) {
			index := int(k.(amoeba.IntKey))
			if index >= end {
				s.Stop()
				return
			}

			batch = append(batch, i.(LogItem))
		})
	})
	return
}

func (d *segment) Append(batch []Event, term int) (max int) {
	if len(batch) == 0 {
		return
	}

	d.raw.Update(func(u amoeba.Update) {
		max = d.Head()
		for _, e := range batch {
			max++
			u.Put(amoeba.IntKey(max), LogItem{max, e, term})
		}

		d.max.Set(max)
	})
	return
}

func (d *segment) Insert(batch []LogItem) {
	if len(batch) == 0 {
		return
	}

	d.raw.Update(func(u amoeba.Update) {
		max := d.Head()
		for _, item := range batch {
			u.Put(amoeba.IntKey(item.Index), item)
			if item.Index > max {
				max = item.Index
			}
		}

		d.max.Set(max)
	})
}

type eventLog struct {
	// the currently active segment.
	active *segment

	// lock around the active segment.  Protects against concurrent
	// updates during compactions.
	activeLock *sync.RWMutex

	// index of last item inserted. (initialized to -1)
	append *position

	// index of last item committed.(initialized to -1)
	commit *position

	// closing utilities.
	closed chan struct{}

	// closing lock.  (a buffered channel of 1 entry.)
	closer chan struct{}
}

func newEventLog(ctx common.Context) *eventLog {
	// closed := make(chan struct{})
	return nil
	// return &eventLog{amoeba.NewBTreeIndex(32), newPosition(closed), newPosition(closed), closed, make(chan struct{}, 1)}
}

func (d *eventLog) Close() error {
	select {
	case <-d.closed:
		return ClosedError
	case d.closer <- struct{}{}:
	}

	d.commit.Notify()
	d.append.Notify()
	close(d.closed)
	return nil
}

func (d *eventLog) Committed() (pos int) {
	return d.commit.Get()
}

func (d *eventLog) Active() *segment {
	d.activeLock.RLock()
	defer d.activeLock.RUnlock()
	return d.active
}

func (d *eventLog) UpdateActive(fn func(s *segment) *segment) {
	d.activeLock.Lock()
	defer d.activeLock.Unlock()
	d.active = fn(d.active)
}

func (d *eventLog) ListenCommits(from int, buf int) (*commitListener, error) {
	select {
	case <-d.closed:
		return nil, ClosedError
	default:
	}

	return newCommitListener(d, from, buf), nil
}

func (d *eventLog) Commit(pos int) {
	d.commit.Update(func(cur int) int {
		return common.Min(pos, d.append.Get())
	})
}

func (d *eventLog) Head() (pos int) {
	return d.Active().Head()
}

func (d *eventLog) Get(index int) (item LogItem, ok bool) {
	return d.Active().Get(index)
}

func (d *eventLog) Scan(start int, end int) (batch []LogItem, err error) {
	return d.Active().Scan(start, end)
}

func (d *eventLog) Append(batch []Event, term int) (index int) {
	return d.Active().Append(batch, term)
}

func (d *eventLog) Insert(batch []LogItem) {
	d.Active().Insert(batch)
}

func (d *eventLog) Snapshot() (index int, term int, commit int) {
	// must take commit prior to taking the end of the log
	// to ensure invariant commit <= head
	commit = d.Committed()

	last := d.Active().Last()
	return last.Index, last.term, commit
}

func (d *eventLog) Compact(snapshot []Event, head int) (err error) {
	d.UpdateActive(func(s *segment) *segment {
		// grab the last item.
		raw, ok := d.Get(head)
		if !ok {
			err = errors.Wrapf(StateError, "Unable to compact to [%v].  It doesn't exist!", head)
			return s
		}

		// copy from current segment from head + 1 on.
		batch, e := s.Scan(head+1, s.max.Get())
		if e != nil {
			err = errors.Wrapf(StateError, "Unable to compact to [%v].  It doesn't exist!", head)
			return s
		}

		// build the compacted items.
		compacted := make([]LogItem, 0, len(snapshot))
		for i, e := range snapshot {
			compacted = append(compacted, LogItem{Index: head - len(snapshot) + i, term: raw.term, Event: e})
		}

		// build the new segment
		new := newSegment(d)
		new.Insert(compacted)
		new.Insert(batch)
		return new
	})
	return
}

type commitListener struct {
	log    *eventLog
	segment *segment
	ch     chan LogItem
	closed chan struct{}
	closer chan struct{}
}

func newCommitListener(log *eventLog, from int, buf int) *commitListener {
	l := &commitListener{log, log.Active(), make(chan LogItem, buf), make(chan struct{}), make(chan struct{}, 1)}
	l.start(from)
	return l
}

func (l *commitListener) start(from int) {
	go func() {
		defer l.Close()

		for cur := from;; {
			next, err := l.log.commit.WaitForChange(from, l.closed)
			if err != nil {
				return
			}

			for {
				log, err := l.segment.Scan(cur, next+1)
				if err != nil {
					return
				}

				for _, i := range log {
					select {
					case <-l.log.closed:
						return
					case <-l.closed:
						return
					case l.ch <- i:
					}
				}

				from = next + 1
			}
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

func newPosition(val int, cancel <-chan struct{}) *position {
	return &position{val, &sync.Cond{L: &sync.Mutex{}}, cancel}
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

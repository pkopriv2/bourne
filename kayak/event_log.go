package kayak

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
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

type eventLog struct {
	// the currently active segment.
	id uuid.UUID

	// logger
	logger common.Logger

	// the stash instanced used for durability
	stash stash.Stash

	//
	raw durableLog

	// the currently active segment.
	active *segment

	// lock around the active segment.  Protects against concurrent updates during compactions.
	activeLock sync.RWMutex

	// // index of last item inserted. (initialized to -1)
	head *position

	// index of last item committed.(initialized to -1)
	commit *position

	// closing utilities.
	closed chan struct{}

	// closing lock.  (a buffered channel of 1 entry.)
	closer chan struct{}
}

func openEventLog(logger common.Logger, stash stash.Stash, id uuid.UUID) (*eventLog, error) {
	raw, err := openDurableLog(stash, id)
	if err != nil {
		return nil, err
	}

	seg, err := raw.Active(stash)
	if err != nil {
		return nil, err
	}

	head, err := seg.Head(stash)
	if err != nil {
		return nil, err
	}

	commit, err := raw.GetCommit(stash)
	if err != nil {
		return nil, err
	}

	return &eventLog{
		stash:  stash,
		raw:    raw,
		logger: logger.Fmt("EventLog"),
		active: &segment{stash, seg},
		head:   newPosition(head),
		commit: newPosition(commit),
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1)}, nil
}

func (e *eventLog) Close() error {
	select {
	case <-e.closed:
		return ClosedError
	case e.closer <- struct{}{}:
	}

	close(e.closed)
	e.commit.Close()
	e.head.Close()
	return nil
}

func (e *eventLog) Closed() bool {
	select {
	case <-e.closed:
		return true
	default:
		return false
	}
}

func (e *eventLog) Committed() (pos int) {
	return e.commit.Get()
}

func (e *eventLog) UseActive(fn func(*segment)) {
	e.activeLock.RLock()
	defer e.activeLock.RUnlock()
	fn(e.active)
}

func (e *eventLog) UpdateActive(fn func(s *segment) *segment) {
	e.activeLock.Lock()
	defer e.activeLock.Unlock()
	e.active = fn(e.active)
}

// NOT SAFE!!
func (e *eventLog) Active() *segment {
	e.activeLock.RLock()
	defer e.activeLock.RUnlock()
	return e.active
}

func (e *eventLog) Size() (int, error) {
	segment := e.Active()
	head, err := segment.Head()
	if err != nil {
		return 0, err
	}

	return head - segment.raw.prevIndex, nil
}

func (e *eventLog) ListenCommits(from int, buf int) *positionListener {
	return newPositionListener(e, e.commit, from, buf)
}

func (e *eventLog) Commit(pos int) (new int, err error) {
	new = e.commit.Update(func(cur int) int {
		if cur >= pos {
			return cur
		}

		pos, err = e.raw.SetCommit(e.stash, common.Min(pos, e.Head()))
		if err != nil {
			return cur
		} else {
			return pos
		}
	})
	return
}

func (e *eventLog) Head() int {
	return e.head.Get()
}

func (e *eventLog) Assert(index int, term int) (ok bool, err error) {
	e.UseActive(func(s *segment) {
		ok, err = s.Assert(index, term)
	})
	return
}

func (e *eventLog) Get(index int) (i LogItem, o bool, f error) {
	e.UseActive(func(s *segment) {
		i, o, f = s.Get(index)
	})
	return
}

func (e *eventLog) Scan(start int, end int) (r []LogItem, f error) {
	e.UseActive(func(s *segment) {
		r, f = s.Scan(start, end)
	})
	return
}

func (e *eventLog) Append(batch []Event, term int) (h int, f error) {
	e.UseActive(func(s *segment) {
		h, f = s.Append(batch, term)
	})
	if f != nil {
		return
	}

	return e.head.Set(h), nil // commutative, so safe for out of order scheduling

}

func (e *eventLog) Insert(batch []LogItem) (h int, f error) {
	e.UseActive(func(s *segment) {
		h, f = s.Insert(batch)
	})
	if f != nil {
		return
	}

	return e.head.Set(h), nil // commutative, so safe for out of order scheduling
}

func (e *eventLog) Max() (i LogItem, f error) {
	var head int
	var ok bool
	e.UseActive(func(s *segment) {
		head, f = s.Head()
		if f != nil {
			i = LogItem{Index: -1, term: -1}
			return
		}

		i, ok, f = s.Get(head)
		if f != nil || !ok {
			i = LogItem{Index: -1, term: -1}
			return
		}
	})
	return
}

func (e *eventLog) Snapshot() (index int, term int, commit int, err error) {
	// // must take commit prior to taking the end of the log
	// // to ensure invariant commit <= head
	commit, active := e.Committed(), e.Active()

	head, err := active.Head()
	if err != nil {
		return -1, -1, -1, err
	}

	item, ok, err := active.Get(head)
	if err != nil || !ok {
		return -1, -1, -1, err
	}

	return item.Index, item.term, commit, nil
}

func (e *eventLog) Compact(until int, snapshot <-chan Event, snapshotSize int, config []byte) (err error) {
	e.logger.Info("Compacting log until [%v] with a snapshot of [%v] events", until, snapshotSize)

	cur := e.Active()

	// Go ahead and compact the majority of the segment. (writes can still happen.)
	new, err := cur.Compact(until, snapshot, snapshotSize, config)
	if err != nil {
		return errors.Wrapf(err, "Error compacting current segment [%v]", cur.raw.id)
	}

	defer cur.Delete()

	// Swap existing with new.
	e.UpdateActive(func(cur *segment) *segment {
		ok, err := e.raw.SwapActive(e.stash, cur.raw, new.raw)
		if err != nil || !ok {
			return cur
		}
		return new
	})

	before, err := cur.Head()
	if err != nil {
		e.logger.Error("Error retrieving before head: %v", err)
	}

	after, err := new.Head()
	if err != nil {
		e.logger.Error("Error retrieving after head: %v", err)
	}

	e.logger.Info("Compaction finished. Before [%v]. After [%v]", before-cur.raw.prevIndex, after-new.raw.prevIndex)
	return
}

type segment struct {
	stash stash.Stash
	raw   durableSegment
}

func (d *segment) Head() (int, error) {
	return d.raw.Head(d.stash)
}

func (d *segment) Assert(index int, term int) (bool, error) {
	return d.raw.Assert(d.stash, index, term)
}

func (d *segment) Get(index int) (LogItem, bool, error) {
	return d.raw.Get(d.stash, index)
}

// scans from [start,end], inclusive on start and end
func (d *segment) Scan(start int, end int) ([]LogItem, error) {
	return d.raw.Scan(d.stash, start, end)
}

func (d *segment) Append(batch []Event, term int) (int, error) {
	return d.raw.Append(d.stash, batch, term)
}

func (d *segment) Insert(batch []LogItem) (int, error) {
	return d.raw.Insert(d.stash, batch)
}

func (d *segment) Delete() error {
	return d.raw.Delete(d.stash)
}

func (d *segment) Compact(until int, snapshot <-chan Event, snapshotSize int, config []byte) (*segment, error) {
	new, err := d.raw.CopyAndCompact(d.stash, until, snapshot, snapshotSize, config)
	if err != nil {
		return nil, err
	}

	return &segment{d.stash, new}, nil
}

type positionListener struct {
	log *eventLog
	pos *position
	buf int
	ch  chan struct {
		LogItem
		error
	}

	closed chan struct{}
	closer chan struct{}
}

func newPositionListener(log *eventLog, pos *position, from int, buf int) *positionListener {
	l := &positionListener{log, pos, buf, make(chan struct {
		LogItem
		error
	}, buf), make(chan struct{}), make(chan struct{}, 1)}
	l.start(from)
	return l
}

func (l *positionListener) start(from int) {
	go func() {
		defer l.Close()

		for cur := from; ; {

			next, ok := l.pos.WaitForGreaterThanOrEqual(cur)
			if !ok || l.isClosed() {
				return
			}

			for cur <= next {

				// scan the next batch
				batch, err := l.log.Scan(cur, common.Min(next, cur+l.buf))

				// start emitting
				for _, i := range batch {
					tuple := struct {
						LogItem
						error
					}{i, err}

					select {
					case <-l.log.closed:
						return
					case <-l.closed:
						return
					case l.ch <- tuple:
						if err != nil {
							return
						}
					}
				}

				// update current
				cur = cur + len(batch)
			}
		}
	}()
}

func (l *positionListener) isClosed() bool {
	select {
	default:
		return false
	case <-l.closed:
		return true
	}
}

func (p *positionListener) Next() (LogItem, error) {
	select {
	case <-p.closed:
		return LogItem{}, ClosedError
	case i := <-p.ch:
		return i.LogItem, i.error
	}
}

func (l *positionListener) Close() error {
	select {
	case <-l.closed:
		return ClosedError
	case l.closer <- struct{}{}:
	}

	// FIXME: This does NOT work!
	close(l.closed)
	l.pos.Notify()
	return nil
}

type position struct {
	val  int
	lock *sync.Cond
	dead bool
}

func newPosition(val int) *position {
	return &position{val, &sync.Cond{L: &sync.Mutex{}}, false}
}

func (c *position) WaitForGreaterThanOrEqual(pos int) (commit int, alive bool) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	if c.dead {
		return pos, false
	}

	for commit, alive = c.val, !c.dead; commit < pos; commit, alive = c.val, !c.dead {
		c.lock.Wait()
		if c.dead {
			return pos, false
		}
	}
	return
}

func (c *position) Notify() {
	c.lock.Broadcast()
}

func (c *position) Close() {
	c.lock.L.Lock()
	defer c.lock.Broadcast()
	defer c.lock.L.Unlock()
	c.dead = true
}

func (c *position) Update(fn func(int) int) int {
	c.lock.L.Lock()
	defer c.lock.Broadcast()
	defer c.lock.L.Unlock()
	c.val = common.Max(fn(c.val), c.val)
	return c.val
}

func (c *position) Set(pos int) (new int) {
	c.lock.L.Lock()
	defer c.lock.Broadcast()
	defer c.lock.L.Unlock()
	c.val = common.Max(pos, c.val)
	return c.val
}

func (c *position) Get() (pos int) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	return c.val
}

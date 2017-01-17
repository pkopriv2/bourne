package kayak

import (
	"sync"

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

	// the stash instanced used for durability
	stash stash.Stash

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

func openEventLog(id uuid.UUID, stash stash.Stash) (*eventLog, error) {
	return nil, nil
}

// func newEventLog(ctx common.Context) *eventLog {
// return &eventLog{
// active: newSegment([]Event{}, -1, -1),
// head:   newPosition(-1),
// commit: newPosition(-1),
// closed: make(chan struct{}),
// closer: make(chan struct{}, 1)}
// }

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

func (e *eventLog) Active() *segment {
	e.activeLock.RLock()
	defer e.activeLock.RUnlock()
	return e.active
}

func (e *eventLog) UpdateActive(fn func(s *segment) *segment) {
	e.activeLock.Lock()
	defer e.activeLock.Unlock()
	e.active = fn(e.active)
}

func (e *eventLog) ListenCommits(from int, buf int) *positionListener {
	return newPositionListener(e, e.commit, from, buf)
}

func (e *eventLog) Commit(pos int) {
	e.commit.Update(func(cur int) int {
		return common.Min(pos, e.head.Get())
	})
}

func (e *eventLog) Head() int {
	return e.head.Get()
}

func (e *eventLog) Get(index int) (LogItem, bool, error) {
	return e.Active().Get(index)
}

func (e *eventLog) Scan(start int, end int) ([]LogItem, error) {
	return e.Active().Scan(start, end)
}

func (e *eventLog) Append(batch []Event, term int) (int, error) {
	head, err := e.Active().Append(batch, term)
	if err != nil {
		return 0, err
	}

	return e.head.Set(head), nil // commutative, so safe for out of order scheduling
}

func (e *eventLog) Insert(batch []LogItem) (int, error) {
	head, err := e.Active().Insert(batch)
	if err != nil {
		return 0, err
	}

	return e.head.Set(head), nil // commutative, so safe for out of order scheduling
}

func (e *eventLog) Snapshot() (index int, term int, commit int) {
	return

	// // must take commit prior to taking the end of the log
	// // to ensure invariant commit <= head
	// commit = e.Committed()
	// last, error := e.Active().Head()
	// return last.Index, last.term, commit
}

func (e *eventLog) Compact(until int, snapshot []Event, config []byte) (err error) {

	// Go ahead and compact the majority of the segment. (writes can still happen.)
	new, err := e.Active().Compact(until, snapshot, config)
	if err != nil {
		return err
	}

	// Swap existing with new.
	e.UpdateActive(func(cur *segment) *segment {
		var curHead int
		var newHead int

		curHead, err = cur.Head()
		if err != nil {
			return cur
		}

		newHead, err = new.Head()
		if err != nil {
			return cur
		}

		var backfill []LogItem
		if curHead > newHead {
			backfill, err = new.Scan(newHead, curHead)
			if err != nil {
				return cur
			}

			newHead, err = new.Insert(backfill)
			if err != nil {
				return cur
			}
		}

		return new
	})
	return
}

type segment struct {
	stash stash.Stash
	raw   durableSegment
}

func firstSegment(db stash.Stash) (*segment, error) {
	return nil, nil
	// var err error
	// var raw durableSegment
	// err = db.Update(func(tx *bolt.Tx) error {
	// raw, err = initDurableSegment(tx, raw.id)
	// return err
	// })
	// if err != nil {
	// return nil, err
	// }
	//
	// return &segment{db, raw}, nil
}

func (d *segment) Head() (head int, err error) {
	// d.stash.View(func(tx *bolt.Tx) error {
	// head, err = d.raw.Head(tx)
	// return err
	// })
	return
}

func (d *segment) Get(index int) (item LogItem, ok bool, err error) {
	// d.stash.View(func(tx *bolt.Tx) error {
	// item, ok, err = d.raw.Get(tx, index)
	// return err
	// })
	return
}

// scans from [start,end], inclusive on start and end
func (d *segment) Scan(start int, end int) (batch []LogItem, err error) {
	// d.stash.View(func(tx *bolt.Tx) error {
	// batch, err = d.raw.Scan(tx, start, end)
	// return err
	// })
	return
}

func (d *segment) Append(batch []Event, term int) (head int, err error) {
	// d.stash.Update(func(tx *bolt.Tx) error {
	// head, err = d.raw.append(tx, batch, term)
	// return err
	// })
	return
}

func (d *segment) Insert(batch []LogItem) (head int, err error) {
	// d.stash.Update(func(tx *bolt.Tx) error {
	// head, err = d.raw.Insert(tx, batch)
	// return err
	// })
	return
}

func (d *segment) Compact(until int, snapshot []Event, config []byte) (seg *segment, err error) {
	return nil, nil
}

type positionListener struct {
	log     *eventLog
	segment *segment
	pos     *position
	ch      chan LogItem
	closed  chan struct{}
	closer  chan struct{}
}

func newPositionListener(log *eventLog, pos *position, from int, buf int) *positionListener {
	l := &positionListener{log, log.Active(), pos, make(chan LogItem, buf), make(chan struct{}), make(chan struct{}, 1)}
	l.start(from)
	return l
}

func (l *positionListener) start(from int) {
	go func() {
		defer l.Close()

		for cur := from; ; {
			if l.isClosed() {
				return
			}

			next, ok := l.pos.WaitForGreaterThanOrEqual(cur)
			if !ok {
				return
			}

			for cur <= next {

				// if we've moved beyond the current segment, reset to active.
				head, err := l.segment.Head()
				if err != nil {
					return
				}

				if cur > head {
					l.segment = l.log.Active()
				}

				// scan the next batch
				batch, err := l.segment.Scan(cur, common.Min(head, next, cur+255))
				if err != nil {
					return
				}

				// start emitting
				for _, i := range batch {
					select {
					case <-l.log.closed:
						return
					case <-l.closed:
						return
					case l.ch <- i:
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

func (l *positionListener) Closed() <-chan struct{} {
	return l.closed
}

func (l *positionListener) Items() <-chan LogItem {
	return l.ch
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
			return pos, true
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

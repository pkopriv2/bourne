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

func openEventLog(stash stash.Stash, id uuid.UUID) (*eventLog, error) {
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

func (e *eventLog) Commit(pos int) (err error) {
	e.commit.Update(func(cur int) int {
		err = e.raw.SetCommit(e.stash, pos)
		if err != nil {
			return cur
		}

		return common.Min(pos, e.head.Get())
	})
	return
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
	cur := e.Active()

	// Go ahead and compact the majority of the segment. (writes can still happen.)
	new, err := cur.Compact(until, snapshot, snapshotSize, config)
	if err != nil {
		return err
	}

	// Swap existing with new.
	e.UpdateActive(func(cur *segment) *segment {
		ok, err := e.raw.SwapActive(e.stash, cur.raw, new.raw)
		if err != nil || !ok {
			return cur
		}
		return new
	})
	return
}

type segment struct {
	stash stash.Stash
	raw   durableSegment
}

func (d *segment) Head() (int, error) {
	return d.raw.Head(d.stash)
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

func (d *segment) Compact(until int, snapshot <-chan Event, snapshotSize int, config []byte) (*segment, error) {
	new, err := d.raw.CopyAndCompact(d.stash, until, snapshot, snapshotSize, config)
	if err != nil {
		return nil, err
	}

	return &segment{d.stash, new}, nil
}

type positionListener struct {
	log    *eventLog
	pos    *position
	ch     chan struct{LogItem; error}
	closed chan struct{}
	closer chan struct{}
}

func newPositionListener(log *eventLog, pos *position, from int, buf int) *positionListener {
	l := &positionListener{log, pos, make(chan struct{LogItem; error}, buf), make(chan struct{}), make(chan struct{}, 1)}
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
				batch, err := l.log.Scan(cur, common.Min(next, cur+255))

				// start emitting
				for _, i := range batch {
					select {
					case <-l.log.closed:
						return
					case <-l.closed:
						return
					case l.ch <- struct{LogItem; error}{i, err}:
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

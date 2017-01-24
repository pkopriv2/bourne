package kayak

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

type eventLog struct {

	// logger
	logger common.Logger

	// the stored log.
	raw StoredLog

	// the currently active segment.
	active StoredSegment

	// lock around the active segment.  Protects against concurrent updates during compactions.
	activeLock sync.RWMutex

	// // index of last item inserted. (initialized to -1)
	head *ref

	// index of last item committed.(initialized to -1)
	commit *ref

	// closing utilities.
	closed chan struct{}

	// closing lock.  (a buffered channel of 1 entry.)
	closer chan struct{}
}

func openEventLog(logger common.Logger, log StoredLog) (*eventLog, error) {
	cur, err := log.Active()
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to retrieve latest segment for log [%v]", log.Id())
	}

	commit, err := log.GetCommit()
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to retrieve commit for log [%v]", log.Id())
	}

	head, err := cur.Head()
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to retrieve head position for segment [%v]", log.Id())
	}

	return &eventLog{
		logger: logger.Fmt("EventLog"),
		active: cur,
		head:   newRef(head),
		commit: newRef(commit),
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

func (e *eventLog) UseActive(fn func(StoredSegment)) {
	e.activeLock.RLock()
	defer e.activeLock.RUnlock()
	fn(e.active)
}

func (e *eventLog) UpdateActive(fn func(StoredSegment) StoredSegment) {
	e.activeLock.Lock()
	defer e.activeLock.Unlock()
	e.active = fn(e.active)
}

// Use with caution
func (e *eventLog) Active() StoredSegment {
	e.activeLock.RLock()
	defer e.activeLock.RUnlock()
	return e.active
}

func (e *eventLog) Head() int {
	return e.head.Get()
}

func (e *eventLog) Committed() (pos int) {
	return e.commit.Get()
}

func (e *eventLog) Commit(pos int) (new int, err error) {
	new = e.commit.Update(func(cur int) int {
		if cur >= pos {
			return cur
		}

		pos, err = e.raw.SetCommit(common.Min(pos, e.Head()))
		if err != nil {
			return cur
		} else {
			return pos
		}
	})
	return
}

func (e *eventLog) Snapshot() (snapshot, error) {
	var err error
	var snap StoredSnapshot
	var prevIndex int
	var prevTerm int
	e.UseActive(func(seg StoredSegment) {
		snap, err = seg.Snapshot()
		if err != nil {
			return
		}

		prevIndex = seg.PrevIndex()
		prevTerm = seg.PrevTerm()
		return
	})
	return snapshot{snap, prevIndex, prevTerm}, err
}

func (e *eventLog) Get(index int) (i LogItem, o bool, f error) {
	e.UseActive(func(s StoredSegment) {
		i, o, f = s.Get(index)
	})
	return
}

func (e *eventLog) Scan(start int, end int) (r []LogItem, f error) {
	e.UseActive(func(s StoredSegment) {
		r, f = s.Scan(start, end)
	})
	return
}

func (e *eventLog) Append(evt Event, term int, source uuid.UUID, seq int, kind int) (h int, f error) {
	e.UseActive(func(s StoredSegment) {
		h, f = s.Append(evt, term, source, seq, kind)
	})
	if f != nil {
		return
	}

	return e.head.Set(h), nil // commutative, so safe for out of order scheduling
}

func (e *eventLog) Insert(batch []LogItem) (h int, f error) {
	e.UseActive(func(s StoredSegment) {
		h, f = s.Insert(batch)
	})
	if f != nil {
		return
	}

	return e.head.Set(h), nil // commutative, so safe for out of order scheduling
}

func (e *eventLog) Compact(until int, snapshot <-chan Event, snapshotSize int, config []byte) (err error) {
	e.logger.Info("Compacting log until [%v] with a snapshot of [%v] events", until, snapshotSize)

	cur := e.Active()

	// Go ahead and compact the majority of the segment. (writes can still happen.)
	new, err := cur.Compact(until, snapshot, snapshotSize, config)
	if err != nil {
		return errors.Wrapf(err, "Error compacting current segment [%v]", cur)
	}

	defer cur.Delete()

	// Swap existing with new.
	e.UpdateActive(func(cur StoredSegment) StoredSegment {
		ok, err := e.raw.Swap(cur, new)
		if err != nil || !ok {
			return cur
		}
		return new
	})
	return
}

func (e *eventLog) Assert(index int, term int) (ok bool, err error) {
	var item LogItem
	e.UseActive(func(s StoredSegment) {
		item, ok, err = e.Get(index)
		if !ok || err != nil {
			return
		}

		ok = item.Term == term
		return
	})
	return
}

func (e *eventLog) Max() (i LogItem, f error) {
	var head int
	var ok bool
	e.UseActive(func(s StoredSegment) {
		head, f = s.Head()
		if f != nil {
			i = LogItem{Index: -1, Term: -1}
			return
		}

		i, ok, f = s.Get(head)
		if f != nil || !ok {
			i = LogItem{Index: -1, Term: -1}
			return
		}
	})
	return
}

func (e *eventLog) ListenCommits(from int, buf int) (Listener, error) {
	if e.Closed() {
		return nil, ClosedError
	}

	return newRefListener(e, e.commit, from, buf), nil
}

func (e *eventLog) ListenAppends(from int, buf int) (Listener, error) {
	if e.Closed() {
		return nil, ClosedError
	}

	return newRefListener(e, e.head, from, buf), nil
}

type snapshot struct {
	raw       StoredSnapshot
	PrevIndex int
	PrevTerm  int
}

func (s *snapshot) Size() int {
	return s.raw.Size()
}

func (s *snapshot) Config() ([]peer,error) {
	return parsePeers(s.raw.Config(), []peer{})
}

func (s *snapshot) Events(cancel <-chan struct{}) <-chan Event {
	ch := make(chan Event)
	go func() {
		defer close(ch)

		for cur := 0; cur < s.raw.Size(); {
			batch, err := s.raw.Scan(cur, common.Min(cur, cur+256))
			if err != nil {
				return
			}

			cur = cur + len(batch)
		}
	}()
	return ch
}


type refListener struct {
	log *eventLog
	pos *ref
	buf int
	ch  chan struct {
		LogItem
		error
	}

	closed chan struct{}
	closer chan struct{}
}

func newRefListener(log *eventLog, pos *ref, from int, buf int) *refListener {
	l := &refListener{log, pos, buf, make(chan struct {
		LogItem
		error
	}, buf), make(chan struct{}), make(chan struct{}, 1)}
	l.start(from)
	return l
}

func (l *refListener) start(from int) {
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

func (l *refListener) isClosed() bool {
	select {
	default:
		return false
	case <-l.closed:
		return true
	}
}

func (p *refListener) Next() (LogItem, error) {
	select {
	case <-p.closed:
		return LogItem{}, ClosedError
	case i := <-p.ch:
		return i.LogItem, i.error
	}
}

func (l *refListener) Close() error {
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

type filteredListener struct {
	raw Listener
	seq map[uuid.UUID]int
}

func newFilteredListener(raw Listener) *filteredListener {
	return &filteredListener{raw, make(map[uuid.UUID]int)}
}

func (p *filteredListener) Next() (LogItem, error) {
	for {
		next, err := p.raw.Next()
		if err != nil {
			return next, err
		}

		cur := p.seq[next.Source]
		if next.Seq <= cur {
			continue
		}

		p.seq[next.Source] = next.Seq
		return next, nil
	}
}

func (l *filteredListener) Close() error {
	return l.raw.Close()
}

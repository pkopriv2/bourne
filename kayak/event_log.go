package kayak

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

type eventLog struct {

	// logger
	logger common.Logger

	// the stored log.
	raw StoredLog

	// index of last item inserted. (initialized to -1)
	head *ref

	// index of last item committed.(initialized to -1)
	commit *ref

	// closing utilities.
	closed chan struct{}

	// closing lock.  (a buffered channel of 1 entry.)
	closer chan struct{}
}

func openEventLog(logger common.Logger, log StoredLog) (*eventLog, error) {
	head, _, err := log.Last()
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to retrieve head position for segment [%v]", log.Id())
	}

	return &eventLog{
		logger: logger.Fmt("EventLog"),
		raw:    log,
		head:   newRef(head),
		commit: newRef(-1),
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

func (e *eventLog) Head() int {
	return e.head.Get()
}

func (e *eventLog) Committed() (pos int) {
	return e.commit.Get()
}

func (e *eventLog) Commit(pos int) (new int, err error) {
	var head int
	new = e.commit.Update(func(cur int) int {
		head, _, err = e.raw.Last()
		if err != nil {
			return cur
		}

		return common.Max(cur, common.Min(pos, head))
	})
	return
}

func (e *eventLog) Get(index int) (LogItem, bool, error) {
	return e.raw.Get(index)
}

func (e *eventLog) Scan(start int, end int) ([]LogItem, error) {
	return e.raw.Scan(start, end)
}

func (e *eventLog) Append(evt Event, term int, source uuid.UUID, seq int, kind int) (LogItem, error) {
	item, err := e.raw.Append(evt, term, source, seq, kind)
	if err != nil {
		return item, err
	}

	e.head.Update(func(cur int) int {
		return common.Max(cur, item.Index)
	})

	return item, nil
}

func (e *eventLog) Insert(batch []LogItem) error {
	if len(batch) == 0 {
		return nil
	}

	if err := e.raw.Insert(batch); err != nil {
		return err
	}

	e.head.Update(func(cur int) int {
		return common.Max(cur, batch[len(batch)-1].Index)
	})
	return nil
}

func (e *eventLog) Truncate(from int) (err error) {
	e.head.Update(func(cur int) int {
		if from >= cur {
			return cur
		}

		err = e.raw.Truncate(from)
		if err != nil {
			return cur
		} else {
			return 0 // Moved all the way back to zero!
		}
	})
	return
}

func (e *eventLog) Compact(until int, snapshot <-chan Event, snapshotSize int, config []byte) (err error) {
	e.logger.Info("Compacting log until [%v] with a snapshot of [%v] events", until, snapshotSize)
	_, err = e.raw.Compact(until, snapshot, snapshotSize, config)
	return
}

func (e *eventLog) Snapshot() (StoredSnapshot, error) {
	return e.raw.Snapshot()
}

// only called from followers.
func (e *eventLog) Assert(index int, term int) (ok bool, err error) {
	var item LogItem
	item, ok, err = e.Get(index)
	if !ok || err != nil {
		return
	}

	return item.Term == term, nil
}

func (e *eventLog) Last() (int, int, error) {
	return e.raw.Last()
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

func (s *snapshot) Config() []byte {
	return s.raw.Config()
}

func (s *snapshot) Events(cancel <-chan struct{}) <-chan Event {
	ch := make(chan Event)
	go func() {
		for cur := 0; cur < s.raw.Size(); {
			batch, err := s.raw.Scan(cur, common.Min(cur, cur+256))
			if err != nil {
				return
			}

			for _, e := range batch {
				select {
				case ch <- e:
				case <-cancel:
					return
				}
			}

			cur = cur + len(batch)
		}

		close(ch)
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

	failure error
	closed  chan struct{}
	closer  chan struct{}
}

func newRefListener(log *eventLog, pos *ref, from int, buf int) *refListener {
	ch := make(chan struct {
		LogItem
		error
	}, buf)

	l := &refListener{
		log:    log,
		pos:    pos,
		buf:    buf,
		ch:     ch,
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1)}
	l.start(from)
	return l
}

func (l *refListener) start(from int) {
	go func() {
		defer l.Close()

		for cur := from; ; {

			next, ok := l.pos.WaitForChange(cur)
			if !ok || l.isClosed() {
				return
			}

			// FIXME: Can still miss truncations
			if next < cur {
				l.shutdown(errors.Wrapf(OutOfBoundsError, "Log truncated to [%v] was [%v]", next, cur))
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
							l.shutdown(err)
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
		return LogItem{}, common.Or(p.failure, ClosedError)
	case i := <-p.ch:
		return i.LogItem, i.error
	}
}

func (l *refListener) Close() error {
	return l.shutdown(nil)
}

func (l *refListener) shutdown(cause error) error {
	select {
	case <-l.closed:
		return ClosedError
	case l.closer <- struct{}{}:
	}

	l.failure = cause
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

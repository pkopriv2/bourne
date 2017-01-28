package kayak

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

type eventLog struct {
	ctx    common.Context
	ctrl   common.Control
	logger common.Logger

	// the stored log.
	raw StoredLog

	// index of last item inserted. (initialized to -1)
	head *ref

	// index of last item committed.(initialized to -1)
	commit *ref
}

func openEventLog(ctx common.Context, log StoredLog) (*eventLog, error) {
	head, _, err := log.Last()
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to retrieve head position for segment [%v]", log.Id())
	}

	ctx = ctx.Sub("EventLog")

	headRef := newRef(head)
	ctx.Control().OnClose(func(error) {
		ctx.Logger().Info("Closing head ref")
		headRef.Close()
	})

	commitRef := newRef(-1)
	ctx.Control().OnClose(func(error) {
		ctx.Logger().Info("Closing commit ref")
		commitRef.Close()
	})

	return &eventLog{
		ctx:    ctx,
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
		raw:    log,
		head:   headRef,
		commit: commitRef}, nil
}

func (e *eventLog) Close() error {
	return e.ctrl.Close()
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
		if from > cur {
			return cur
		}

		err = e.raw.Truncate(from)
		if err != nil {
			return cur
		} else {
			return from - 1
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
func (e *eventLog) Assert(index int, term int) (bool, error) {
	item, ok, err := e.Get(index)
	if err != nil {
		return false, err
	}

	if ok {
		return item.Term == term, nil
	}

	s, err := e.Snapshot()
	if err != nil {
		return false, err
	}

	return s.LastIndex() == index && s.LastTerm() == term, nil
}

func (e *eventLog) Last() (int, int, error) {
	return e.raw.Last()
}

func (e *eventLog) ListenCommits(from int, buf int) (Listener, error) {
	if e.ctrl.IsClosed() {
		return nil, ClosedError
	}

	return newRefListener(e, e.commit, from, buf), nil
}

func (e *eventLog) ListenAppends(from int, buf int) (Listener, error) {
	if e.ctrl.IsClosed() {
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
	log  *eventLog
	pos  *ref
	buf  int
	ch   chan LogItem
	ctrl common.Control
}

func newRefListener(log *eventLog, pos *ref, from int, buf int) *refListener {
	l := &refListener{
		log:  log,
		pos:  pos,
		buf:  buf,
		ch:   make(chan LogItem, buf),
		ctrl: common.NewControl(nil),
	}
	l.start(from)
	return l
}

func (l *refListener) start(from int) {
	go func() {
		defer l.Close()

		for cur := from; ; {
			next, ok := l.pos.WaitExceeds(cur)
			if !ok || l.ctrl.IsClosed() || l.log.ctrl.IsClosed() {
				return
			}

			// FIXME: Can still miss truncations
			if next < cur {
				l.ctrl.Fail(errors.Wrapf(OutOfBoundsError, "Log truncated to [%v] was [%v]", next, cur))
				return
			}

			for cur <= next {
				if l.ctrl.IsClosed() || l.log.ctrl.IsClosed() {
					return
				}

				// scan the next batch
				batch, err := l.log.Scan(cur, common.Min(next, cur+l.buf))
				if err != nil {
					l.ctrl.Fail(err)
					return
				}

				// start emitting
				for _, i := range batch {
					select {
					case <-l.log.ctrl.Closed():
						return
					case <-l.ctrl.Closed():
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

func (p *refListener) Next() (LogItem, bool, error) {
	select {
	case <-p.ctrl.Closed():
		return LogItem{}, false, p.ctrl.Failure()
	case i := <-p.ch:
		return i, true, nil
	}
}

func (l *refListener) Close() error {
	return l.ctrl.Close()
}

type filteredListener struct {
	raw Listener
	seq map[uuid.UUID]int
}

func newFilteredListener(raw Listener) *filteredListener {
	return &filteredListener{raw, make(map[uuid.UUID]int)}
}

func (p *filteredListener) Next() (LogItem, bool, error) {
	for {
		next, ok, err := p.raw.Next()
		if err != nil || !ok {
			return next, ok, err
		}

		cur := p.seq[next.Source]
		if next.Seq <= cur {
			continue
		}

		p.seq[next.Source] = next.Seq
		return next, true, nil
	}
}

func (l *filteredListener) Close() error {
	return l.raw.Close()
}

package kayak

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

// FIXME: Split snapshot creating from the

// NOTE: With regard to error handling, there are specific error causes that consumers
// will be looking for.  Therefore, do NOT decorate any errors that are the result
// of the StoredLog operation.

type eventLog struct {
	ctx    common.Context
	ctrl   common.Control
	logger common.Logger
	raw    StoredLog
	head   *ref
	commit *ref
}

func openEventLog(ctx common.Context, log StoredLog) (*eventLog, error) {
	head, _, err := log.Last()
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to retrieve head position for segment [%v]", log.Id())
	}

	ctx = ctx.Sub("EventLog")

	headRef := newRef(head)
	ctx.Control().Defer(func(error) {
		ctx.Logger().Info("Closing head ref")
		headRef.Close()
	})

	commitRef := newRef(-1)
	ctx.Control().Defer(func(error) {
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

func (e *eventLog) Get(index int) (Entry, bool, error) {
	return e.raw.Get(index)
}

func (e *eventLog) Scan(start int, end int) ([]Entry, error) {
	return e.raw.Scan(start, end)
}

func (e *eventLog) Append(evt Event, term int, k Kind) (Entry, error) {
	item, err := e.raw.Append(evt, term, k)
	if err != nil {
		return item, err
	}

	e.head.Update(func(cur int) int {
		return common.Max(cur, item.Index)
	})

	return item, nil
}

func (e *eventLog) Insert(batch []Entry) error {
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

func (e *eventLog) NewSnapshot(lastIndex int, lastTerm int, ch <-chan Event, size int, config []byte) (StoredSnapshot, error) {
	store, err := e.raw.Store()
	if err != nil {
		return nil, err
	}

	return store.NewSnapshot(lastIndex, lastTerm, ch, size, config)
}

func (e *eventLog) Install(snapshot StoredSnapshot) error {
	err := e.raw.Install(snapshot)
	if err != nil {
		return err
	}

	last := snapshot.LastIndex()
	e.head.Update(func(cur int) int {
		if cur < last {
			return last
		} else {
			return cur
		}
	})

	return err
}

func (e *eventLog) Compact(until int, data <-chan Event, size int, config []byte) error {
	item, ok, err := e.Get(until)
	if err != nil || !ok {
		return errors.Wrapf(CompactionError, "Cannot compact until [%v].  It doesn't exist", until)
	}

	snapshot, err := e.NewSnapshot(item.Index, item.Term, data, size, config)
	if err != nil {
		return err
	}

	return e.raw.Install(snapshot)
}

func (e *eventLog) Last() (int, int, error) {
	return e.raw.Last()
}

func (e *eventLog) ListenCommits(from int, buf int) (Listener, error) {
	if e.ctrl.IsClosed() {
		return nil, errors.WithStack(ClosedError)
	}

	return newRefListener(e, e.commit, from, buf), nil
}

func (e *eventLog) ListenAppends(from int, buf int) (Listener, error) {
	if e.ctrl.IsClosed() {
		return nil, errors.WithStack(ClosedError)
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
	log    *eventLog
	pos    *ref
	buf    int
	ch     chan Entry
	ctrl   common.Control
	logger common.Logger
}

func newRefListener(log *eventLog, pos *ref, from int, buf int) *refListener {
	ctx := log.ctx.Sub("Listener")

	l := &refListener{
		log:    log,
		pos:    pos,
		buf:    buf,
		ch:     make(chan Entry, buf),
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
	}
	l.start(from)
	return l
}

func (l *refListener) start(from int) {
	go func() {
		defer l.Close()

		for beg := from; ; {
			// l.logger.Debug("Next [%v]", beg)

			next, ok := l.pos.WaitUntil(beg)
			if !ok || l.ctrl.IsClosed() || l.log.ctrl.IsClosed() {
				return
			}

			// l.logger.Debug("Update: [%v->%v]", beg, next)

			// FIXME: Can still miss truncations
			if next < beg {
				l.ctrl.Fail(errors.Wrapf(OutOfBoundsError, "Log truncated to [%v] was [%v]", next, beg))
				return
			}

			for beg <= next {
				if l.ctrl.IsClosed() || l.log.ctrl.IsClosed() {
					return
				}

				// scan the next batch
				batch, err := l.log.Scan(beg, common.Min(next, beg+l.buf))
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
				beg = beg + len(batch)
			}
		}
	}()
}

func (p *refListener) Data() <-chan Entry {
	return p.ch
}

func (p *refListener) Ctrl() common.Control {
	return p.ctrl
}

func (l *refListener) Close() error {
	return l.ctrl.Close()
}

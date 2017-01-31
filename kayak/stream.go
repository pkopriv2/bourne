package kayak

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

func NewEventChannel(arr []Event) <-chan Event {
	ch := make(chan Event)
	go func() {
		defer close(ch)
		for _, i := range arr {
			ch <- i
		}
	}()
	return ch
}

func CollectEvents(ch <-chan Event, exp int) ([]Event, error) {
	ret := make([]Event, 0, exp)
	for i := 1; i <= exp; i++ {
		e, ok := <-ch
		if !ok {
			return nil, errors.Wrapf(EndOfStreamError, "Expected [%v] events.  Only received [%v]", exp, i)
		}

		ret = append(ret, e)
	}
	return ret, nil
}

func streamSnapshot(ctrl common.Control, snapshot StoredSnapshot, buf int) *snapshotStream {
	return newSnapshotStream(ctrl, snapshot, buf)
}

type snapshotStream struct {
	ctrl     common.Control
	snapshot StoredSnapshot
	buf      int
	ch       chan Event
}

func newSnapshotStream(ctrl common.Control, snapshot StoredSnapshot, buf int) *snapshotStream {
	l := &snapshotStream{
		ctrl:     ctrl.Sub(),
		snapshot: snapshot,
		buf:      buf,
		ch:       make(chan Event, buf),
	}
	l.start()
	return l
}

func (l *snapshotStream) start() {
	go func() {
		defer l.Close()

		for i := 0; i < l.snapshot.Size(); i++ {
			beg := i
			end := common.Min(l.snapshot.Size()-1, beg+l.buf)

			// scan the next batch
			batch, err := l.snapshot.Scan(beg, end)
			if err != nil {
				l.ctrl.Fail(err)
				return
			}

			// start emitting
			for _, e := range batch {
				select {
				case <-l.ctrl.Closed():
					return
				case l.ch <- e:
				}
			}

			// update current
			i = i + len(batch)
		}
	}()
}

func (p *snapshotStream) Next() (Event, error) {
	select {
	case <-p.ctrl.Closed():
		return nil, p.ctrl.Failure()
	case e := <-p.ch:
		return e, nil
	}
}

func (l *snapshotStream) Close() error {
	return l.ctrl.Close()
}

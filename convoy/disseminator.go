package convoy

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/emirpasic/gods/trees/binaryheap"
	"github.com/pkopriv2/bourne/common"
)

var DisseminatorClosedError = errors.New("DISSEMINATOR:CLOSED")

// Sends an update to a randomly chosen recipient.

// The disseminator implements the most crucial aspects of epidemic algorithms.
//
// Based on analysis by [1], a randomly connected graph of N nodes becomes fully
// connected once, log(N)/f = 1, where f is the number of edges between random
// pairs of nodes. Therefore, to reach total dissemination, each update must be
// disseminated to at least, f = log(N) peersi.
//
// However, there are other considerations that must be taken into account, namely
//
//	* How do we avoid missing transiently failed nodes?
//  * How do we avoid overloading underlying network resources?
//
// [1] http://se.inf.ethz.ch/old/people/eugster/papers/gossips.pdf
//
type Disseminator interface {
	Push(update) error
	Close() error
}

type disseminatorImpl struct {
	roster  Roster
	updates Updates

	period  time.Duration
	timeout time.Duration
	batch   int

	closed chan struct{}
	closer chan struct{}
	wait   sync.WaitGroup
}

func newDisseminator(ctx common.Context, r Roster) Disseminator {
	ret := &disseminatorImpl{
		roster:  r,
		updates: newPendingUpdates(),
		timeout: ctx.Config().OptionalDuration(confUpdateTimeout, defaultUpdateTimeout),
		batch:   ctx.Config().OptionalInt(confUpdateBatchSize, defaultUpdateBatchSize),
		period:  ctx.Config().OptionalDuration(confDisseminationPeriod, defaultDisseminationPeriod),
		closed:  make(chan struct{}),
		closer:  make(chan struct{}, 1)}

	go disseminate(ret)
	return ret
}

func (d *disseminatorImpl) Push(u update) error {
	remaining := int(math.Ceil(math.Log10(float64(d.roster.Size()))))
	return d.push([]pending{pending{u, remaining}})
}

func (d *disseminatorImpl) push(p []pending) error {
	for _, cur := range p {
		select {
		case <-d.closed:
			return DisseminatorClosedError
		case d.updates.Push() <- cur:
		}
	}

	return nil
}

func (d *disseminatorImpl) pop() ([]pending, error) {
	ret := make([]pending, 0, d.batch)

	for i := 0; i < d.batch; i++ {
		select {
		default:
			break
		case <-d.closed:
			return nil, DisseminatorClosedError
		case p := <-d.updates.Pop():
			ret = append(ret, p)
		}
	}

	return ret, nil
}


func (d *disseminatorImpl) Close() error {
	select {
	case <-d.closed:
		return nil
	case d.closer <- struct{}{}:
	}

	close(d.closed)
	d.wait.Wait()
	return nil
}


// func (d *disseminatorImpl) fail(m Member) error {
// if !d.roster.(m.Id(), m.Version()) {
// return nil
// }
//
// return d.Push(newFail(m.Id(), m.Version()))
// }

func disseminate(d *disseminatorImpl) {
	defer d.wait.Done()

	gen := Generate(d.roster, d.closed)

	timer := time.NewTimer(d.period)
	for range timer.C  {
		// var m Member
		select {
		case <-d.closed:
		case _ = <-gen:
		}

		// client, err := m.client()
		// if err != nil {
			// continue
		// }
//
		// batch, err := d.pop()
		// if err != nil {
			// return
		// }
//
		// updates := make([]update, 0, len(batch))
		// for _, u := range batch {
			// updates = append(updates, u.update)
		// }
	}

}

// The list of pending updates tracks the updates to be
// disemminated amongst the group.  It attempts to favor
// updates which have not been fully disemminated, by
// tracking the number of remaining attempts for an update.
//
// TODO: Currently implemented as unbounded queue.  If we bound,
// then a potential deadlock exists in the dissemination logic
// where commiting a batch back onto the queue can be blocked,
// halting the disseminator indefinitely.
type Updates interface {
	Close() error
	Push() chan<- pending
	Pop() <-chan pending
}

type pending struct {
	update    update
	remaining int
}

type updatesQueue struct {
	heap   *binaryheap.Heap
	push   chan pending
	pop    chan pending
	closer chan struct{}
	closed chan struct{}
	wait   sync.WaitGroup
}

func newPendingUpdates() Updates {
	u := &updatesQueue{heap: binaryheap.NewWith(maxRemainingComparator)}
	u.wait.Add(2)
	go popper(u)
	go pusher(u)
	return u
}

func (u *updatesQueue) Close() error {
	select {
	case <-u.closed:
		return fmt.Errorf("Updates queue closed")
	case u.closer <- struct{}{}:
	}

	close(u.closed)
	u.wait.Wait()
	return nil
}

func (u *updatesQueue) Push() chan<- pending {
	return u.push
}

func (u *updatesQueue) Pop() <-chan pending {
	return u.pop
}

func pusher(p *updatesQueue) {
	defer p.wait.Done()

	for {
		var pending pending
		select {
		case <-p.closed:
			return
		case pending = <-p.push:
		}

		p.heap.Push(pending)
	}
}

func popper(p *updatesQueue) {
	defer p.wait.Done()

	for {

		val, _ := p.heap.Pop()
		select {
		case <-p.closed:
			return
		case p.pop <- val.(pending):
		}
	}
}

func maxRemainingComparator(a, b interface{}) int {
	pendingA := a.(pending)
	pendingB := b.(pending)

	return pendingB.remaining - pendingA.remaining
}

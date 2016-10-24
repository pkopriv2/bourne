package convoy

import (
	"fmt"
	"sync"
	"time"

	"github.com/emirpasic/gods/trees/binaryheap"
	"github.com/pkopriv2/bourne/common"
)

// Sends an update to a randomly chosen recipient.

// The disseminator implements the most crucial aspects of epidemic algorithms.
//
// Based on analysis by [1], a randomly connected graph of N nodes becomes fully
// connected once, log(N)/f = 1, where f is the number of edges between each
// node.  Therefore, every update, must attempt to infect at least, f = log(N) peers
// in order for total infection.
//
// However, there are other considerations that must be taken into account, namely
//
//	* How do we avoid missing transiently failed nodes?
//  * How do we avoid overloading
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

	timeout time.Duration

	closed chan struct{}
	closer chan struct{}
	wait   sync.WaitGroup
}

func newDisseminator(ctx common.Context, r Roster) Disseminator {
	ret := &disseminatorImpl{
		roster:  r,
		updates: newPendingUpdates(),
		timeout: ctx.Config().OptionalDuration(confUpdateTimeout, defaultUpdateTimeout),
		closed:  make(chan struct{}),
		closer:  make(chan struct{}, 1)}

	go disseminate(ret)
	return ret
}

func (d *disseminatorImpl) Push(u update) error {
	select {
	case <-d.closed:
		return fmt.Errorf("Unable to push update [%v]. Disseminator closed.")
	case d.updates.Push() <- pending{u, 0}:
		return nil
	}
}

func (d *disseminatorImpl) push(p pending) error {
	select {
	case <-d.closer:
		return fmt.Errorf("Unable to push update [%v]. Disseminator closing.")
	case <-d.closed:
		return fmt.Errorf("Unable to push update [%v]. Disseminator closed.")
	case d.updates.Push() <- p:
		return nil
	}
}

func (d *disseminatorImpl) pop() (pending, error) {
	var p pending
	select {
	case <-d.closer:
		return p, fmt.Errorf("Unable to pop update. Disseminator closing.")
	case <-d.closed:
		return p, fmt.Errorf("Unable to pop update. Disseminator closed.")
	case p = <-d.updates.Pop():
		return p, nil
	}
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

func (d *disseminatorImpl) retry(p pending) error {
	return d.push(p)
}

func (d *disseminatorImpl) succeed(p pending) error {
	if p.remaining == 0 {
		return nil
	}

	return d.push(pending{p.update, p.remaining - 1})
}

// func (d *disseminatorImpl) fail(m Member) error {
	// if !d.roster.fail(m.Id(), m.Version()) {
		// return nil
	// }
//
	// return d.Push(newFail(m.Id(), m.Version()))
// }

func disseminate(d *disseminatorImpl) {
	defer d.wait.Done()

	gen := Generate(d.roster)
	for {
		cur, err := d.pop()
		if err != nil {
			return
		}

		m := <-gen // no need for synchronized select...

		client, err := m.client()
		if err != nil {
			d.retry(cur)
			continue
		}

		_, err = client.Update(cur.update, d.timeout)
		if err != nil {
			d.retry(cur)
			continue
		}

		// successful dissemination happens regardless of whether
		// the recipient had already seen the information...
		d.succeed(cur)
	}
}

// The list of pending updates tracks the updates to be
// disemminated amongst the group.  It attempts to favor
// updates which have not been fully disemminated, by
// tracking the number of dissemination/gossip attempts
// that have failed.
//
// TODO: Define semantics for a full queue!!
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
	size   chan struct{}
	closer chan struct{}
	closed chan struct{}
	wait   sync.WaitGroup
}

func newPendingUpdates() Updates {
	u := &updatesQueue{heap: binaryheap.NewWith(minIgnoresComparator)}
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
		select {
		case <-p.closed:
			return
		case p.size <- struct{}{}:
		}

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
		select {
		case <-p.closed:
			return
		case <-p.size:
		}

		val, _ := p.heap.Pop()
		select {
		case <-p.closed:
			return
		case p.pop <- val.(pending):
		}
	}
}

func minIgnoresComparator(a, b interface{}) int {
	pendingA := a.(pending)
	pendingB := b.(pending)

	return pendingA.remaining - pendingB.remaining
}

package convoy

import (
	"fmt"
	"sync"
	"time"

	"github.com/emirpasic/gods/trees/binaryheap"
)

type peer struct {
	roster    Roster
	updates   Updates
	disperser disperser

	closer chan struct{}
	closed chan struct{}
	wait   sync.WaitGroup
}

func (p *peer) Roster() Roster {
	return p.roster
}

func (p *peer) Update(u update) bool {
	ret := u.Apply(p.roster)
	p.updates.Push() <- pending{u, 0}
	return ret
}

func disseminate(p *peer) {
	defer p.wait.Done()

	pop := p.updates.Pop()
	for {
		var cur pending
		select {
		case <-p.closed:
			return
		case cur = <-pop:
		}
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
	update   update
	ignored int
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
	u := &updatesQueue{heap: binaryheap.NewWith(minFailuresComparator)}
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

	return pendingA.ignored - pendingB.ignored
}

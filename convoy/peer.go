package convoy

import (
	"sync"

	"github.com/emirpasic/gods/trees/binaryheap"
)

// The list of pending updates tracks the updates to be
// disemminated amongst the group.  It attempts to favor
// updates which have not been fully disemminated, by
// tracking the number of dissemination/gossip attempts
// for which the
type PendingUpdates interface {
	Push(Update, int)
	Pop() (Update, int)
}

type peer struct {
	roster       Roster
	updates      PendingUpdates
	disseminator disperser
}

func (p *peer) Roster() Roster {
	return p.roster
}

func (p *peer) Update(update Update) {
	update.Apply(p.roster)
	p.updates.Push(update, 0)
}

type pendingUpdate struct {
	update   Update
	failures int
}

type pendingUpdates struct {
	lock sync.Mutex
	heap *binaryheap.Heap
}

func newPendingUpdates() PendingUpdates {
	return &pendingUpdates{heap: binaryheap.NewWith(minFailuresComparator)}
}

func (p *pendingUpdates) Push(u Update, cnt int) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.heap.Push(pendingUpdate{u, cnt})
}

func (p *pendingUpdates) Pop() (Update, int) {
	p.lock.Lock()
	defer p.lock.Unlock()
	val, ok := p.heap.Pop()
	if !ok {
		return nil, 0 // empty
	}

	v := val.(pendingUpdate)
	return v.update, v.failures
}

func minFailuresComparator(a, b interface{}) int {
	updateA := a.(pendingUpdate)
	updateB := b.(pendingUpdate)

	return updateA.failures - updateB.failures
}

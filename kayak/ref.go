package kayak

import (
	"sync"

	"github.com/pkopriv2/bourne/common"
)

type ref struct {
	val  int
	lock *sync.Cond
	dead bool
}

func newRef(val int) *ref {
	return &ref{val, &sync.Cond{L: &sync.Mutex{}}, false}
}

func (c *ref) WaitForGreaterThanOrEqual(pos int) (commit int, alive bool) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	if c.dead {
		return pos, false
	}

	for commit, alive = c.val, !c.dead; commit < pos; commit, alive = c.val, !c.dead {
		c.lock.Wait()
		if c.dead {
			return pos, false
		}
	}
	return
}

func (c *ref) Notify() {
	c.lock.Broadcast()
}

func (c *ref) Close() {
	c.lock.L.Lock()
	defer c.lock.Broadcast()
	defer c.lock.L.Unlock()
	c.dead = true
}

func (c *ref) Update(fn func(int) int) int {
	c.lock.L.Lock()
	defer c.lock.Broadcast()
	defer c.lock.L.Unlock()
	c.val = common.Max(fn(c.val), c.val)
	return c.val
}

func (c *ref) Set(pos int) (new int) {
	c.lock.L.Lock()
	defer c.lock.Broadcast()
	defer c.lock.L.Unlock()
	c.val = common.Max(pos, c.val)
	return c.val
}

func (c *ref) Get() (pos int) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	return c.val
}

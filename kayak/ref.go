package kayak

import (
	"sync"

	"github.com/pkopriv2/bourne/common"
)

type position struct {
	val  int
	lock *sync.Cond
	dead bool
}

func newPosition(val int) *position {
	return &position{val, &sync.Cond{L: &sync.Mutex{}}, false}
}

func (c *position) WaitForGreaterThanOrEqual(pos int) (commit int, alive bool) {
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

func (c *position) Notify() {
	c.lock.Broadcast()
}

func (c *position) Close() {
	c.lock.L.Lock()
	defer c.lock.Broadcast()
	defer c.lock.L.Unlock()
	c.dead = true
}

func (c *position) Update(fn func(int) int) int {
	c.lock.L.Lock()
	defer c.lock.Broadcast()
	defer c.lock.L.Unlock()
	c.val = common.Max(fn(c.val), c.val)
	return c.val
}

func (c *position) Set(pos int) (new int) {
	c.lock.L.Lock()
	defer c.lock.Broadcast()
	defer c.lock.L.Unlock()
	c.val = common.Max(pos, c.val)
	return c.val
}

func (c *position) Get() (pos int) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	return c.val
}

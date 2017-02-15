package kayak

import (
	"sync"

	"github.com/pkopriv2/bourne/common"
)

// A simply notifying integer value.

type ref struct {
	val  int
	lock *sync.Cond
	dead bool
}

func newRef(val int) *ref {
	return &ref{val, &sync.Cond{L: &sync.Mutex{}}, false}
}

func (c *ref) WaitExceeds(cur int) (val int, alive bool) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	for val, alive = c.val, !c.dead; val <= cur && alive; val, alive = c.val, !c.dead {
		c.lock.Wait()
	}
	return
}

func (c *ref) WaitUntil(cur int) (val int, alive bool) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	for val, alive = c.val, !c.dead; val < cur && alive; val, alive = c.val, !c.dead {
		c.lock.Wait()
	}
	return
}

func (c *ref) WaitUntilOrCancel(cancel <-chan struct{}, until int) (val int, alive bool) {
	go func() {
		<-cancel
		c.Notify()
	}()
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	for {
		val, alive, canceled := c.val, !c.dead, common.IsCanceled(cancel)
		if val >= until || !alive || canceled {
			return val, alive
		}
		c.lock.Wait()
	}
}

func (c *ref) Notify() {
	c.lock.Broadcast()
}

func (c *ref) Close() {
	c.lock.L.Lock()
	defer c.lock.Broadcast()
	defer c.lock.L.Unlock()
	c.dead = true
	c.lock.Broadcast()
}

func (c *ref) Update(fn func(int) int) int {
	var cur int
	var ret int
	defer func() {
		if cur != ret {
			c.lock.Broadcast()
		}
	}()

	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	cur = c.val
	ret = fn(c.val)
	c.val = ret
	return ret
}

func (c *ref) Set(pos int) int {
	return c.Update(func(int) int {
		return pos
	})
}

func (c *ref) Get() (pos int) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	return c.val
}

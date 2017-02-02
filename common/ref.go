package common

import (
	"sync"
)

// A simply notifying integer value.

type Ref struct {
	val  int
	lock *sync.Cond
	dead bool
}

func NewRef(val int) *Ref {
	return &Ref{val, &sync.Cond{L: &sync.Mutex{}}, false}
}

func (c *Ref) WaitExceeds(cur int) (val int, alive bool) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	for val, alive = c.val, !c.dead; val <= cur && alive; val, alive = c.val, !c.dead {
		c.lock.Wait()
	}
	return
}

func (c *Ref) WaitUntil(cur int) (val int, alive bool) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	for val, alive = c.val, !c.dead; val < cur && alive; val, alive = c.val, !c.dead {
		c.lock.Wait()
	}
	return
}

func (c *Ref) WaitUntilOrCancel(cancel <-chan struct{}, until int) (val int, alive bool) {
	go func() {
		<-cancel
		c.Notify()
	}()
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	for {
		val, alive, canceled := c.val, !c.dead, IsCanceled(cancel)
		if val >= until || !alive || canceled {
			return val, alive
		}
		c.lock.Wait()
	}
}

func (c *Ref) Notify() {
	c.lock.Broadcast()
}

func (c *Ref) Close() {
	c.lock.L.Lock()
	defer c.lock.Broadcast()
	defer c.lock.L.Unlock()
	c.dead = true
	c.lock.Broadcast()
}

func (c *Ref) Update(fn func(int) int) int {
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

func (c *Ref) Set(pos int) int {
	return c.Update(func(int) int {
		return pos
	})
}

func (c *Ref) Get() (pos int) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	return c.val
}

package common

import (
	"sync"
)

// A simply notifying integer value.

type WaterMark struct {
	val  int
	lock *sync.Cond
	dead bool
}

func NewWaterMark(val int) *WaterMark {
	return &WaterMark{val, &sync.Cond{L: &sync.Mutex{}}, false}
}

func (c *WaterMark) WaitExceeds(cur int) (val int, alive bool) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	for val, alive = c.val, !c.dead; val <= cur && alive; val, alive = c.val, !c.dead {
		c.lock.Wait()
	}
	return
}

func (c *WaterMark) WaitUntil(cur int) (val int, alive bool) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	for val, alive = c.val, !c.dead; val < cur && alive; val, alive = c.val, !c.dead {
		c.lock.Wait()
	}
	return
}

func (c *WaterMark) WaitUntilOrCancel(cancel <-chan struct{}, until int) (val int, alive bool) {
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

func (c *WaterMark) Notify() {
	c.lock.Broadcast()
}

func (c *WaterMark) Close() error {
	c.lock.L.Lock()
	defer c.lock.Broadcast()
	defer c.lock.L.Unlock()
	c.dead = true
	c.lock.Broadcast()
	return nil
}

func (c *WaterMark) Update(fn func(int) int) int {
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

func (c *WaterMark) Set(pos int) int {
	return c.Update(func(int) int {
		return pos
	})
}

func (c *WaterMark) Get() (pos int) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	return c.val
}

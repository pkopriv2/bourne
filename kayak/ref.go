package kayak

import "sync"

// A simply notifying integer value.

type ref struct {
	val  int
	lock *sync.Cond
	dead bool
}

func newRef(val int) *ref {
	return &ref{val, &sync.Cond{L: &sync.Mutex{}}, false}
}

func (c *ref) WaitForChange(cur int) (val int, alive bool) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	if c.dead {
		return cur, false
	}

	for val, alive = c.val, !c.dead; val == cur && alive; val, alive = c.val, !c.dead {
		c.lock.Wait()
		// if c.dead {
		// return cur, false
		// }
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
	defer c.lock.L.Unlock()
	bef := c.val
	c.val = fn(c.val)
	if bef != c.val {
		defer c.lock.Broadcast()
	}
	return c.val
}

func (c *ref) Set(pos int) (new int) {
	return c.Update(func(int) int {
		return pos
	})
}

func (c *ref) Get() (pos int) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	return c.val
}

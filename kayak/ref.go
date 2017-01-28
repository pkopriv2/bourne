package kayak

import (
	"fmt"
	"sync"
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

// func (c *ref) Wait(cur int) (val int, alive bool) {
	// c.lock.L.Lock()
	// defer c.lock.L.Unlock()
	// // if c.dead {
	// // return cur, false
	// // }
//
	// for val, alive = c.val, !c.dead; val == cur && alive; val, alive = c.val, !c.dead {
		// c.lock.Wait()
	// }
	// return
// }

func (c *ref) WaitExceeds(cur int) (val int, alive bool) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	// if c.dead {
		// fmt.Println("DONE WAITING INNER: ", val, alive)
		// return cur, false
	// }
//
	for val, alive = c.val, !c.dead; val <= cur && alive; val, alive = c.val, !c.dead {
		fmt.Println("WAITING:", val, alive)
		c.lock.Wait()
		fmt.Println("WAKEUP")
	}
	fmt.Println("DONE WAITING!: ", val, alive)
	return
}

func (c *ref) WaitUntil(cur int) (val int, alive bool) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	if c.dead {
		fmt.Println("DONE WAITING INNER: ", val, alive)
		return cur, false
	}

	for val, alive = c.val, !c.dead; val < cur && alive; val, alive = c.val, !c.dead {
		fmt.Println("WAITING:", val, alive)
		c.lock.Wait()
		fmt.Println("WAKEUP")
	}
	fmt.Println("DONE WAITING!: ", val, alive)
	return
}

func (c *ref) Notify() {
	c.lock.Broadcast()
}

func (c *ref) Close() {
	c.lock.L.Lock()
	defer c.lock.Broadcast()
	defer c.lock.L.Unlock()
	fmt.Println("REF CLOSED")
	c.dead = true
}

func (c *ref) Update(fn func(int) int) int {
	c.lock.L.Lock()
	defer c.lock.Broadcast()
	defer c.lock.L.Unlock()
	c.val = fn(c.val)
	return c.val
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

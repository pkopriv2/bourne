package convoy

import "github.com/pkopriv2/bourne/concurrent"

// a trivial implementation!!!
// TODO: MAKE THE REAL ONE THAT IS DURABLE!!!
type clock struct {
	counter concurrent.AtomicCounter
}

func (c *clock) Cur() int {
	return int(c.counter.Get())
}

func (c *clock) Inc() int {
	return int(c.counter.Inc())
}

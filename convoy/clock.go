package convoy

import "github.com/pkopriv2/bourne/concurrent"

// a trivial implementation!!!
// TODO: MAKE THE REAL ONE THAT IS DURABLE!!!
type clockImpl struct {
	counter concurrent.AtomicCounter
}

func (c *clockImpl) Cur() int {
	return int(c.counter.Get())
}

func (c *clockImpl) Inc() int {
	return int(c.counter.Inc())
}

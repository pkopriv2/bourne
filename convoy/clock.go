package convoy

import "github.com/pkopriv2/bourne/concurrent"

// the primary reconciliation technique will involve a "globally unique"
// counter for each member.  Luckily, we can distribute the counter to
// the members themselves, allowing us no consistency issues.
type clock interface {
	Cur() int
	Inc() int
}

// a trivial implementation!!!
// TODO: MAKE THE REAL ONE THAT IS DURABLE!!!
type cclock struct {
	counter concurrent.AtomicCounter
}

func (c *cclock) Cur() int {
	return int(c.counter.Get())
}

func (c *cclock) Inc() int {
	return int(c.counter.Inc())
}

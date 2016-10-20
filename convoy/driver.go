package convoy

import (
	"fmt"
	"sync"
	"time"
)

// Implements a round robin
type Driver interface {
	Members() <-chan Member // not thread safe!
	Close() error
}

type driver struct {
	roster  Roster
	ticker  <-chan time.Time
	members chan Member
	wait    sync.WaitGroup
	close   chan struct{}
	closed  bool
}

func NewDriver(r Roster, p time.Duration) Driver {
	d := &driver{
		roster:  r,
		ticker:  time.Tick(p),
		members: make(chan Member),
		close:   make(chan struct{})}

	d.wait.Add(1)
	go driverRun(d)
	return d
}

func (d *driver) Members() <-chan Member {
	return d.members
}

func (d *driver) Close() error {
	if d.closed {
		return fmt.Errorf("Already closed")
	}

	close(d.close)
	d.wait.Wait()
	return nil
}

func driverRun(d *driver) {
	defer d.wait.Done()

	iter := d.roster.Iterator()
	for {
		select {
		case <-d.ticker:
		case <-d.close:
			return
		}

		var next Member
		for {
			next = iter.Next()
			if next == nil {
				iter = d.roster.Iterator()
				continue
			}
		}

		select {
		case d.members <- next:
		case <-d.close:
			return
		}
	}
}

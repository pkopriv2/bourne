package convoy

import (
	"fmt"
	"sync"
	"time"
)

// Implements a round robin, random permutation over
type generator interface {
	Members() <-chan Member // not thread safe!
	Close() error
}

type gen struct {
	roster  Roster
	members chan Member
	wait    sync.WaitGroup
	close   chan struct{}
	closed  bool
}

func NewGenerator(r Roster, p time.Duration) generator {
	d := &gen{
		roster:  r,
		members: make(chan Member),
		close:   make(chan struct{})}

	d.wait.Add(1)
	go genRun(d)
	return d
}

func (d *gen) Members() <-chan Member {
	return d.members
}

func (d *gen) Close() error {
	if d.closed {
		return fmt.Errorf("Already closed")
	}

	close(d.close)
	d.wait.Wait()
	close(d.members)
	return nil
}

func genRun(d *gen) {
	defer d.wait.Done()

	iter := d.roster.Iterator()
	for {
		select {
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

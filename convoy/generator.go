package convoy

import (
	"fmt"
	"sync"
	"time"
)

// Implements a round robin, random permutation over
type Generator interface {
	Members() <-chan Member // not thread safe!
	Close() error
}

type generator struct {
	roster  Roster
	members chan Member
	wait    sync.WaitGroup
	close   chan struct{}
	closed  bool
}

func NewGenerator(r Roster, p time.Duration) Generator {
	d := &generator{
		roster:  r,
		members: make(chan Member),
		close:   make(chan struct{})}

	d.wait.Add(1)
	go generatorRun(d)
	return d
}

func (d *generator) Members() <-chan Member {
	return d.members
}

func (d *generator) Close() error {
	if d.closed {
		return fmt.Errorf("Already closed")
	}

	close(d.close)
	d.wait.Wait()
	close(d.members)
	return nil
}

func generatorRun(d *generator) {
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

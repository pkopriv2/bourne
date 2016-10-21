package convoy

import (
	"time"

	"github.com/pkopriv2/bourne/concurrent"
)

// Sends an update to a randomly chosen recipient.
type disperser interface {
	Send(update, time.Duration) (bool, error)
}

type disperserImpl struct {
	gen  generator
	pool concurrent.WorkPool
}

func newDisperser(g generator, p concurrent.WorkPool) disperser {
	return &disperserImpl{g, p}
}

func (d *disperserImpl) Send(u update, t time.Duration) (bool, error) {
	// selects a random member
	m := <-d.gen.Members()

	// go ahead and run the client.
	val := make(chan interface{})
	if err := d.pool.Submit(val, sendUpdate(m, u, t)); err != nil {
		return false, err
	}

	switch t := (<-val).(type) {
	default:
		panic("Unknown type")
	case bool:
		return t, nil
	case error:
		return false, t
	}
}

func sendUpdate(m Member, u update, t time.Duration) concurrent.Work {
	return func(resp chan<- interface{}) {
		client, err := m.client()
		if err != nil {
			resp <- err
			return
		}

		success, err := client.Update(u, t)
		if err != nil {
			resp <- err
			return
		}

		resp <- success
	}
}

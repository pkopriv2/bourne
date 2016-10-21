package convoy

import (
	"time"

	"github.com/pkopriv2/bourne/concurrent"
)

// Sends an update to a randomly chosen recipient.
type disperser interface {
	Send(Update) (bool, error)
}

type disp struct {
	gen  generator
	pool concurrent.WorkPool
}

func (d *disp) Send(u Update, timeout time.Duration) (bool, error) {
	member := <-d.gen.Members()

	// go ahead and run the client.
	val := make(chan interface{})
	if err := d.pool.Submit(val, SendUpdate(member, u, timeout)); err != nil {
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

func SendUpdate(member Member, update Update, timeout time.Duration) concurrent.Work {
	return func(resp chan<- interface{}) {
		client, err := member.Client()
		if err != nil {
			resp <- err
			return
		}

		success, err := client.update(update, timeout)
		if err != nil {
			resp <- err
			return
		}

		resp <- success
	}
}

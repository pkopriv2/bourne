package convoy

import (
	"sync"
	"time"

	"github.com/pkopriv2/bourne/common"
)

const (
	confConvoyPeriod = "convoy.Period"
)

type Driver interface {
	Members() <-chan Member // not thread safe!
	Close()
}

type driver struct {
	roster Roster
	ticker <-chan time.Time
	members chan Member
	close   chan struct{}
	wait    sync.WaitGroup
}

func NewDriver(ctx common.Context, r Roster) Driver {
	d := &driver{
		roster: r,
		ticker: time.Tick(ctx.Config().OptionalDuration())
	}

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
		case d.members<-next:
		case <-d.close:
			return
		}
	}
}

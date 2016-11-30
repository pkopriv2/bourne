package convoy

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

// Sends an update to a randomly chosen recipient.
//
// The timeLog implements the most crucial aspects of epidemic algorithms.
//
// Based on analysis by [1], a randomly connected graph of N nodes becomes fully
// connected once, log(N)/f = 1, where f is the number of edges between random
// pairs of nodes. Therefore, to reach total timeLogination, each update must be
// timeLoginated to at least, f = log(N) peersi.
//
// However, there are other considerations that must be taken into account, namely
//
// * How do we avoid missing transiently failed nodes?
// * How do we avoid overloading underlying network resources?
//
// [1] http://se.inf.ethz.ch/old/people/eugster/papers/gossips.pdf

// timeLog implementation.
type timeLog struct {
	Ctx    common.Context
	Logger common.Logger
	Evts   *eventLog
	Ttl    time.Duration
	Period time.Duration
	Closed chan struct{}
	Closer chan struct{}
	Wait   sync.WaitGroup
}

func newTimeLog(ctx common.Context, logger common.Logger, ttl time.Duration) (*timeLog, error) {
	ret := &timeLog{
		Ctx:    ctx,
		Logger: logger.Fmt("TimeLog"),
		Evts:   newEventLog(ctx),
		Ttl:    ttl,
		Period: ttl / 10,
		Closed: make(chan struct{}),
		Closer: make(chan struct{}, 1)}

	if err := ret.start(); err != nil {
		return nil, err
	}

	return ret, nil
}

func (d *timeLog) Close() error {
	select {
	case <-d.Closed:
		return nil
	case d.Closer <- struct{}{}:
	}

	close(d.Closed)
	d.Wait.Wait()
	return nil
}

func (d *timeLog) Peek() []event {
	return eventLogExtractEvents(d.Evts.Peek(1024))
}

func (d *timeLog) Push(e []event) error {
	select {
	default:
	case <-d.Closed:
		return errors.Errorf("Disseminator closed")
	}

	num := len(e)
	if num == 0 {
		return nil
	}

	d.Evts.Add(e, int(time.Now().Unix()))
	return nil
}

func (d *timeLog) start() error {
	d.Wait.Add(1)
	go func() {
		defer d.Wait.Done()

		tick := time.NewTicker(d.Period)
		for {
			select {
			case <-d.Closed:
				return
			case <-tick.C:
			}

			start := time.Now()
			batch := d.Evts.Pop(1024)
			if batch == nil {
				continue
			}

			alive := make([]eventLogEntry, 0, len(batch))
			for _, e := range batch {
				if start.Sub(time.Unix(int64(e.Key.Weight), 0)) < d.Ttl {
					alive = append(alive, e)
				}
			}

			d.Evts.Return(alive)
		}
	}()

	return nil
}

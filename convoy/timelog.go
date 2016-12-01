package convoy

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

// Maintains a time sorted list of events up to some specified maximum ttl.
//
// TODO: Make TTL configurable based on cluster

// timeLog implementation.
type timeLog struct {
	Ctx    common.Context
	Logger common.Logger
	Evts   *eventLog
	Dir    *directory // need to continuously know cluster size
	Period time.Duration
	Closed chan struct{}
	Closer chan struct{}
	Wait   sync.WaitGroup
}

func newTimeLog(ctx common.Context, logger common.Logger, dir *directory) (*timeLog, error) {
	return &timeLog{
		Ctx:    ctx,
		Logger: logger.Fmt("TimeLog"),
		Evts:   newEventLog(ctx),
		Dir:    dir,
		Closed: make(chan struct{}),
		Closer: make(chan struct{}, 1)}, nil
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

func (d *timeLog) Pop() []event {
	ret := d.Evts.Pop(2048)
	if ret == nil || len(ret) == 0 {
		return []event{}
	}

	defer d.Evts.Return(dissemDecWeight(ret))
	return eventLogExtractEvents(ret)
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

	n := len(d.Dir.All())
	if fanout := dissemFanout(n); fanout > 0 {
		d.Logger.Debug("Adding [%v] events to be disseminated [%v/%v] times", num, fanout, n)
		d.Evts.Add(e, 8 * fanout)
	}

	return nil
}

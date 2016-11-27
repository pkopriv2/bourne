package convoy

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

// Sends an update to a randomly chosen recipient.
//
// The disseminator implements the most crucial aspects of epidemic algorithms.
//
// Based on analysis by [1], a randomly connected graph of N nodes becomes fully
// connected once, log(N)/f = 1, where f is the number of edges between random
// pairs of nodes. Therefore, to reach total dissemination, each update must be
// disseminated to at least, f = log(N) peersi.
//
// However, there are other considerations that must be taken into account, namely
//
// * How do we avoid missing transiently failed nodes?
// * How do we avoid overloading underlying network resources?
//
// [1] http://se.inf.ethz.ch/old/people/eugster/papers/gossips.pdf

type dissemIter struct {
	rest []*member
}

func dissemNewIter(all []*member) *dissemIter {
	return &dissemIter{all}
}

func (i *dissemIter) Next() (m *member) {
	if len(i.rest) == 0 {
		return nil
	}

	m = i.rest[0]
	i.rest = i.rest[1:]
	return
}

// Helper functions.
func dissemShuffleMembers(arr []*member) []*member {
	ret := make([]*member, len(arr))
	for i, j := range rand.Perm(len(arr)) {
		ret[i] = arr[j]
	}
	return ret
}

func dissemNumTransmissions(numMembers int) int {
	return int(math.Ceil(math.Log2(float64(numMembers))))
}

// disseminator implementation.
type disseminator struct {
	ctx    common.Context
	logger common.Logger
	log    *eventLog
	dir    *directory
	period time.Duration
	closed chan struct{}
	closer chan struct{}
	wait   sync.WaitGroup
	size   *atomic.Value
}

func newDisseminator(ctx common.Context, logger common.Logger, self *member, dir *directory, period time.Duration) (*disseminator, error) {
	size := new(atomic.Value)
	size.Store(len(dir.All()))

	ret := &disseminator{
		logger: logger.Fmt("Dissem"),
		log:    newEventLog(ctx),
		dir:    dir,
		period: period,
		size:   size,
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1)}

	if err := ret.start(self); err != nil {
		return nil, err
	}

	return ret, nil
}

func (d *disseminator) Close() error {
	select {
	case <-d.closed:
		return nil
	case d.closer <- struct{}{}:
	}

	close(d.closed)
	d.wait.Wait()
	return nil
}

func (d *disseminator) Push(e []event) error {
	select {
	default:
	case <-d.closed:
		return errors.Errorf("Disseminator closed")
	}

	size := len(d.dir.All())
	n := dissemNumTransmissions(size)
	d.logger.Debug("Adding [%v] events to be disseminated [%v/%v] times", len(e), n, size)
	d.log.PushBatch(e, n)
	return nil
}

func (d *disseminator) start(self *member) error {
	select {
	default:
	case <-d.closed:
		return errors.Errorf("Disseminator closed")
	}

	d.wait.Add(1)
	go func() {
		defer d.wait.Done()

		tick := time.NewTicker(d.period)
		iter := d.newIterator()

		for {
			// get member
			var m *member
			for {
				m = iter.Next()
				if m == nil {
					iter = d.newIterator()
					continue
				}

				if m == self {
					continue
				}

				break
			}

			select {
			case <-d.closed:
				return
			case <-tick.C:
			}

			d.log.Process(d.newProcessor(m))
		}
	}()

	return nil
}

func (d *disseminator) newProcessor(m *member) func([]event) error {
	return func(batch []event) error {
		d.logger.Info("Disseminating [%v] events", len(batch))

		client, err := m.Client(d.ctx)
		if err != nil  {
			return errors.Wrapf(err, "Error retrieving member client [%v]", m)
		}

		if client == nil  {
			return errors.Errorf("Unable to retrieve member client [%v]", m)
		}

		_, err = client.DirApply(batch)
		return err
	}
}

func (d *disseminator) newIterator() *dissemIter {
	return dissemNewIter(dissemShuffleMembers(d.dir.All()))
}

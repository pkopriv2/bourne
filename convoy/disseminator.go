package convoy

import (
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
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


// Accepts a channel of events and disseminates them
func dissemEvents(ch <-chan event, dissem *disseminator) {
	go func() {
		for e := range ch {
			done, timeout := concurrent.NewBreaker(365*24*time.Hour, func() interface{} {
				dissem.Push([]event{e})
				return nil
			})

			select {
			case <-done:
				continue
			case <-timeout:
				return
			}
		}
	}()
}


// A simple iterator
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
// disseminator implementation.
type disseminator struct {
	Ctx    common.Context
	Logger common.Logger
	Evts   *eventLog
	Dir    *directory
	Period time.Duration
	Closed chan struct{}
	Closer chan struct{}
	Wait   sync.WaitGroup
}

func newDisseminator(ctx common.Context, logger common.Logger, self *member, dir *directory, period time.Duration) (*disseminator, error) {
	ret := &disseminator{
		Ctx:    ctx,
		Logger: logger.Fmt("Disseminator"),
		Evts:   newEventLog(ctx),
		Dir:    dir,
		Period: period,
		Closed: make(chan struct{}),
		Closer: make(chan struct{}, 1)}

	if err := ret.start(self); err != nil {
		return nil, err
	}

	return ret, nil
}

func (d *disseminator) Close() error {
	select {
	case <-d.Closed:
		return nil
	case d.Closer <- struct{}{}:
	}

	close(d.Closed)
	d.Wait.Wait()
	return nil
}

func (d *disseminator) Push(e []event) error {
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
		d.Evts.PushBatch(e, fanout)
	}

	return nil
}

func (d *disseminator) start(self *member) error {
	d.Wait.Add(1)
	go func() {
		defer d.Wait.Done()

		tick := time.NewTicker(d.Period)
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

				if m.Id == self.Id {
					continue
				}

				break
			}

			select {
			case <-d.Closed:
				return
			case <-tick.C:
			}

			if err := d.Evts.Process(d.newProcessor(m)); err != nil {
				d.Logger.Debug("Error disseminating: %v", err)
			}
		}
	}()

	return nil
}

func (d *disseminator) newProcessor(m *member) func([]event) error {
	return func(batch []event) error {
		if len(batch) == 0 {
			return nil
		}

		d.Logger.Info("Disseminating [%v] events to member [%v]", len(batch), m)

		client, err := m.Client(d.Ctx)
		if err != nil {
			return errors.Wrapf(err, "Error retrieving member client [%v]", m)
		}

		if client == nil {
			return errors.Errorf("Unable to retrieve member client [%v]", m)
		}

		defer client.Close()
		_, err = client.DirApply(batch)
		return err
	}
}

func (d *disseminator) newIterator() *dissemIter {
	return dissemNewIter(dissemShuffleMembers(d.Dir.All()))
}


// Helper functions.
func dissemShuffleMembers(arr []*member) []*member {
	ret := make([]*member, len(arr))
	for i, j := range rand.Perm(len(arr)) {
		ret[i] = arr[j]
	}
	return ret
}

func dissemFanout(numMembers int) int {
	return 2 * int(math.Ceil(math.Log(float64(numMembers))))
}

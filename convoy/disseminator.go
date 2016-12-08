package convoy

import (
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

var (
	dissemLastSeenThreshold = 8 * int(math.Log(1024)) // puts a soft limit of cluster size
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
func dissemEvents(ch <-chan []event, dissem *disseminator) {
	go func() {
		for batch := range ch {
			dissem.Push(batch)
		}
	}()
}

// A simple member iterator
type dissemIter struct {
	rest []*member
}

func dissemNewIter(all []*member) *dissemIter {
	return &dissemIter{all}
}

func (i *dissemIter) Size() int {
	return len(i.rest)
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
	Evts   *viewLog
	Dir    *directory
	Self   *member
	Iter   *dissemIter
	Lock   sync.Mutex
	Period time.Duration
	Factor int
	Closed chan struct{}
	Closer chan struct{}
	Wait   sync.WaitGroup
}

func newDisseminator(ctx common.Context, logger common.Logger, self *member, dir *directory, period time.Duration) (*disseminator, error) {
	ret := &disseminator{
		Ctx:    ctx,
		Logger: logger.Fmt("Disseminator"),
		Evts:   newViewLog(ctx),
		Dir:    dir,
		Self:   self,
		Period: period,
		Factor: ctx.Config().OptionalInt("convoy.dissem.fanout.factor", 4),
		Closed: make(chan struct{}),
		Closer: make(chan struct{}, 1)}

	if err := ret.start(); err != nil {
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
	if fanout := dissemFanout(d.Factor, n); fanout > 0 {
		d.Logger.Debug("Adding [%v] events to be disseminated [%v/%v] times", num, fanout, n)
		d.Evts.Push(e, fanout)
	}

	return nil
}

func (d *disseminator) nextMember() *member {
	d.Lock.Lock()
	defer d.Lock.Unlock()

	var iter *dissemIter
	var m *member

	iter = d.Iter
	if iter == nil {
		iter = d.newIterator()
	}

	for {
		m = iter.Next()
		if m == nil {
			iter = d.newIterator()
			continue
		}

		if m.Id == d.Self.Id {
			continue
		}

		// if ! m.Healthy {
			// continue
		// }

		break
	}

	return m
}

func (d *disseminator) start() error {
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

			if _, err := d.disseminate(d.nextMember()); err == nil {
				continue
			}

			// TODO: Handle error!
		}
	}()

	return nil
}

func (d *disseminator) disseminate(m *member) ([]event, error) {
	batch := d.Evts.Pop(256)
	d.Logger.Debug("Disseminating events [%v]", len(batch))
	if len(batch) == 0 {
		return batch, nil
	}

	return batch, d.disseminateTo(m, batch)
}

func (d *disseminator) disseminateTo(m *member, batch []event) error {
	client, err := m.Client(d.Ctx)
	if err != nil {
		return errors.Wrapf(err, "Error retrieving member client [%v]", m)
	}

	if client == nil {
		return errors.Errorf("Unable to retrieve member client [%v]", m)
	}

	defer client.Close()

	_, events, err := client.EvtPushPull(batch)
	if err != nil {
		return errors.Wrapf(err, "Error pushing events", m)
	}

	d.Dir.ApplyAll(events)
	return nil
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

func dissemFanout(factor int, numMembers int) int {
	return factor * int(math.Ceil(math.Log(float64(numMembers)))) + 1
}

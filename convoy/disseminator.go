package convoy

import (
	"math"
	"math/rand"
	"sync"
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
	rest []member
}

func dissemNewIter(all []member) *dissemIter {
	return &dissemIter{all}
}

func (i *dissemIter) Size() int {
	return len(i.rest)
}

func (i *dissemIter) Next() (m member, ok bool) {
	if len(i.rest) == 0 {
		return member{}, false
	}

	m = i.rest[0]
	i.rest = i.rest[1:]
	return m, true
}

// disseminator implementation.
type disseminator struct {
	Ctx    common.Context
	Logger common.Logger
	Evts   *viewLog
	Dir    *directory
	Self   member
	Iter   *dissemIter
	Lock   sync.Mutex
	Period time.Duration
	Factor int
	Closed chan struct{}
	Closer chan struct{}
	Wait   sync.WaitGroup
}

func newDisseminator(ctx common.Context, logger common.Logger, self member, dir *directory, period time.Duration) (*disseminator, error) {
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

	n := len(d.Dir.Active())
	if fanout := dissemFanout(d.Factor, n); fanout > 0 {
		d.Logger.Debug("Adding [%v] events to be disseminated [%v/%v] times", num, fanout, n)
		d.Evts.Push(e, fanout)
	}

	return nil
}

func (d *disseminator) nextMember() (member, bool) {
	d.Lock.Lock()
	defer d.Lock.Unlock()

	var iter *dissemIter
	defer func() {
		d.Iter = iter
	}()

	iter = d.Iter
	if iter == nil {
		iter = d.newIterator()
	}

	for {
		if iter == nil {
			return member{}, false
		}

		if m, ok := iter.Next(); ok {
			return m, true
		}

		iter = d.newIterator()
	}
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

			m, ok := d.nextMember()
			if ! ok {
				continue
			}

			if _, err := d.disseminate(m); err != nil {
				d.Dir.Fail(m)
			}
		}
	}()

	return nil
}

func (d *disseminator) disseminate(m member) ([]event, error) {
	batch := d.Evts.Pop(256)
	return batch, d.disseminateTo(m, batch)
}

func (d *disseminator) disseminateTo(m member, batch []event) error {
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

	d.Dir.Apply(events)
	return nil
}

func (d *disseminator) newIterator() *dissemIter {
	members := membersCollect(d.Dir.Healthy(), func(m member) bool {
		return m.Id != d.Self.Id
	})

	if len(members) == 0 {
		return nil
	}

	return dissemNewIter(dissemShuffleMembers(members))
}

// Helper functions.
func dissemShuffleMembers(arr []member) []member {
	ret := make([]member, len(arr))
	for i, j := range rand.Perm(len(arr)) {
		ret[i] = arr[j]
	}
	return ret
}

func dissemFanout(factor int, numMembers int) int {
	return factor*int(math.Ceil(math.Log(float64(numMembers)))) + 1
}

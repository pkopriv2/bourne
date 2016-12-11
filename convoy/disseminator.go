package convoy

import (
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	uuid "github.com/satori/go.uuid"
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
	rest []uuid.UUID
	dir  *directory
}

func dissemNewIter(ids []uuid.UUID, dir *directory) *dissemIter {
	return &dissemIter{ids, dir}
}

func (i *dissemIter) Size() int {
	return len(i.rest)
}

func (i *dissemIter) Next() (m member, ok bool) {
	for !ok {
		if len(i.rest) == 0 {
			return member{}, false
		}

		id := i.rest[0]
		i.rest = i.rest[1:]
		m, ok = i.dir.Get(id)
	}
	return
}

// disseminator implementation.
type disseminator struct {
	Ctx    common.Context
	Logger common.Logger
	Self   member
	Evts   *viewLog
	Dir    *directory
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
		Self:   self,
		Evts:   newViewLog(ctx),
		Dir:    dir,
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

	n := len(d.Dir.AllActive())
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
			if !ok {
				continue
			}

			_, err := d.disseminate(m)
			if err == nil {
				continue
			}

			if err == replicaFailureError {
				d.Logger.Error("Received failure response from [%v].  Evicting self.", m)
				d.Dir.Fail(d.Self)
				continue
			}

			done, timeout := concurrent.NewBreaker(10*time.Second, func() interface{} {
				for {
					ch := d.probe(m, 3)
					if <-ch {
						return true
					}
					if <-ch {
						return true
					}
					return <-ch
				}
			})

			var success bool
			select {
			case <-d.Closed:
				return
			case <-timeout:
				continue
			case e := <-done:
				success = e.(bool)
			}

			if success {
				d.Logger.Info("Successfully probed member [%v]", m)
				continue
			}

			d.Logger.Info("Detected failed member [%v]: %v", m, err)
			d.Dir.Fail(m)
		}

	}()

	return nil
}

func (d *disseminator) probe(m member, num int) <-chan bool {
	iter := d.newIterator()
	if iter == nil {
		ret := make(chan bool, num)
		for i := 0; i < num; i++ {
			ret <- false
		}
		return ret
	}

	ret := make(chan bool, num)
	for i := 0; i < num; i++ {
		proxy, ok := iter.Next()
		if !ok {
			ret <- false
		}

		go func() {
			ok, _ = d.tryProxyPing(m, proxy)
			ret <- ok
		}()
	}
	return ret
}

func (d *disseminator) tryProxyPing(target member, via member) (bool, error) {
	client, err := via.Client(d.Ctx)
	if err != nil || client == nil {
		return false, errors.Wrapf(err, "Error retrieving member client [%v]", via)
	}
	defer client.Close()
	return client.PingProxy(target.Id)
}

func (d *disseminator) disseminate(m member) ([]event, error) {
	batch := d.Evts.Pop(1024)
	return batch, d.disseminateTo(m, batch)
}

func (d *disseminator) disseminateTo(m member, batch []event) error {
	client, err := m.Client(d.Ctx)
	if err != nil || client == nil {
		return errors.Wrapf(err, "Error retrieving member client [%v]", m)
	}

	defer client.Close()
	_, events, err := client.PushPull(d.Self.Id, batch)
	if err != nil {
		return err
	}

	d.Dir.Apply(events)
	return nil
}

func (d *disseminator) newIterator() *dissemIter {
	var ids []uuid.UUID
	var total int
	d.Dir.Core.View(func(v *view) {
		ids = storageHealthCollect(v.Health, func(id uuid.UUID, h health) bool {
			if id == d.Self.Id {
				return false
			}

			m, ok := v.Roster[id]
			if !ok {
				return false
			}

			total++
			return m.Active && h.Healthy
		})
	})

	if len(ids) == 0 {

		// if we get into a state where the rest of the cluster is failed...it's
		// very likely we're in a network partition.  the disseminator won't be
		// able to detect this state and will spin indefinitely, never being
		// able to recover...we need to fail to bail out
		if total > 0 {
			d.Dir.Fail(d.Self)
		}

		return nil
	}

	// delaying retrieval of member until actual dissemination time...
	// so we can cut down on the number of failures while membership
	// is volatile...
	return dissemNewIter(dissemShuffleIds(ids), d.Dir)
}

// Helper functions.
func dissemShuffleIds(arr []uuid.UUID) []uuid.UUID {
	ret := make([]uuid.UUID, len(arr))
	for i, j := range rand.Perm(len(arr)) {
		ret[i] = arr[j]
	}
	return ret
}

func dissemFanout(factor int, numMembers int) int {
	return factor * int(math.Ceil(math.Log(float64(numMembers))))
}

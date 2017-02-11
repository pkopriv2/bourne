package convoy

import (
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
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

// A simple member iterator.  Noticed some very non-performant scheduling on
// high workloads when using a separate member channel.  I think we can merge
// the iterator with the "ticker" to make sure we get a nice long execution
// thread.
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
	ctx          common.Context
	logger       common.Logger
	ctrl         common.Control
	net          net.Network
	self         member
	events       *viewLog
	dir          *directory
	iter         *dissemIter
	dissemPeriod time.Duration
	dissemFactor int
	probeCount   int
	probeTimeout time.Duration
	lock         sync.Mutex
}

func newDisseminator(ctx common.Context, net net.Network, self member, dir *directory) (*disseminator, error) {
	ctx = ctx.Sub("Disseminator")

	ret := &disseminator{
		ctx:          ctx,
		logger:       ctx.Logger(),
		ctrl:         ctx.Control(),
		net:          net,
		self:         self,
		events:       newViewLog(ctx),
		dir:          dir,
		dissemPeriod: ctx.Config().OptionalDuration(Config.DisseminationPeriod, defaultDisseminationPeriod),
		dissemFactor: ctx.Config().OptionalInt(Config.DisseminationFactor, defaultDisseminationFactor),
		probeCount:   ctx.Config().OptionalInt(Config.HealthProbeCount, defaultHealthProbeCount),
		probeTimeout: ctx.Config().OptionalDuration(Config.HealthProbeTimeout, defaultHealthProbeTimeout),
	}

	if err := ret.start(); err != nil {
		return nil, err
	}

	return ret, nil
}

func (d *disseminator) Close() error {
	return d.ctrl.Close()
}

func (d *disseminator) Push(e []event) error {
	if common.IsClosed(d.ctrl.Closed()) {
		return errors.WithStack(ClosedError)
	}

	num := len(e)
	if num == 0 {
		return nil
	}

	n := len(d.dir.AllActive())
	if fanout := dissemFanout(d.dissemFactor, n); fanout > 0 {
		d.logger.Debug("Adding [%v] events to be disseminated [%v] times", num, n)
		d.events.Push(e, fanout)
	}

	return nil
}

// TODO: Inline this into dissem loop
func (d *disseminator) nextMember() (member, bool) {
	d.lock.Lock()
	defer d.lock.Unlock()

	var iter *dissemIter
	defer func() {
		d.iter = iter
	}()

	iter = d.iter
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
	go func() {
		defer d.Close()

		tick := time.NewTicker(d.dissemPeriod)
		for {
			select {
			case <-d.ctrl.Closed():
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

			d.logger.Info("Received failure response from [%v]: %+v.", m, err)
			if cause := common.Extract(err, FailedError); cause == FailedError {
				d.logger.Error("Received failure response from [%v].  Evicting self.", m)
				d.dir.Fail(d.self)
				return
			}

			timer := d.ctx.Timer(30 * time.Second)

			ch := d.probe(m, d.probeCount)
			for i := 0; i < d.probeCount; i++ {
				if timer.IsClosed() {
					d.logger.Info("Detected failed member [%v]: %v", m, err)
					d.dir.Fail(m)
					break
				}

				if <-ch {
					d.logger.Info("Successfully probed member [%v]", m)
					timer.Close()
					break
				}
			}
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
			ok, _ = d.tryPingProxy(m, proxy)
			ret <- ok
		}()
	}
	return ret
}

func (d *disseminator) tryPingProxy(target member, via member) (bool, error) {
	client, err := via.Client(d.ctx, d.net, 30*time.Second)
	if err != nil || client == nil {
		return false, errors.Wrapf(err, "Error retrieving member client [%v]", via)
	}
	defer client.Close()
	return client.PingProxy(target.id)
}

func (d *disseminator) disseminate(m member) ([]event, error) {
	batch := d.events.Pop(1024)
	return batch, d.disseminateTo(m, batch)
}

func (d *disseminator) disseminateTo(m member, batch []event) error {
	client, err := m.Client(d.ctx, d.net, 30*time.Second)
	if err != nil || client == nil {
		return errors.Wrapf(err, "Error retrieving member client [%v]", m)
	}

	defer client.Close()
	_, events, err := client.DissemPushPull(d.self.id, d.self.version, batch)
	if err != nil {
		return err
	}

	d.dir.Apply(events)
	return nil
}

func (d *disseminator) newIterator() *dissemIter {
	var ids []uuid.UUID
	d.dir.Core.View(func(v *view) {
		ids = storageHealthCollect(v.Health, func(id uuid.UUID, h health) bool {
			if id == d.self.id {
				return false
			}

			m, ok := v.Roster[id]
			if !ok {
				return false
			}

			return m.Active && h.Healthy
		})
	})

	if len(ids) == 0 {
		return nil
	}

	// delaying retrieval of member until actual dissemination time...
	// so we can cut down on the number of failures while membership
	// is volatile...
	return dissemNewIter(dissemShuffleIds(ids), d.dir)
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
	return 1 + factor*int(math.Ceil(math.Log(float64(numMembers))))
}

package kayak

import (
	"io"
	"sync"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/scribe"
)

type ReplicatedCounter interface {
	io.Closer

	Inc() (int, error)
	Dec() (int, error)
	Get() int
	Swap(int, int) (bool, error)
}

type swapEvent struct {
	Prev int
	Next int
}

func (c swapEvent) Write(w scribe.Writer) {
	w.WriteInt("prev", c.Prev)
	w.WriteInt("next", c.Next)
}

func swapEventParser(r scribe.Reader) (Event, error) {
	var evt swapEvent
	var err error

	err = common.Or(err, r.ReadInt("prev", &evt.Prev))
	err = common.Or(err, r.ReadInt("next", &evt.Next))
	return evt, err
}

type swapRequest struct {
	swapEvent
	ack chan swapResponse
}

func newSwapRequest(cur int, next int) *swapRequest {
	return &swapRequest{swapEvent{cur, next}, make(chan swapResponse, 1)}
}

func (c *swapRequest) reply(success bool, err error) {
	c.ack <- swapResponse{success, err}
}

type swapResponse struct {
	success bool
	err     error
}

type counterValue struct {
	swapEvent
	index int
}

type counter struct {
	ctx       common.Context
	value     counterValue
	valueLock *sync.Mutex
	subs      concurrent.Map
	commits   chan counterValue
	swaps     chan *swapRequest
	swapPool  concurrent.WorkPool
	closed    chan struct{}
	closer    chan struct{}
}

func NewReplicatedCounter(ctx common.Context, port int, peers []string) (ReplicatedCounter, error) {
	counter := &counter{
		ctx: ctx,
	}
	return counter, nil
}

func (r *counter) Close() error {
	select {
	case <-r.closed:
		return ClosedError
	case r.closer <- struct{}{}:
	}

	close(r.closed)
	return nil
}

func (c *counter) Get() int {
	return c.val().Next
}

func (c *counter) Context() common.Context {
	return c.ctx
}

func (c *counter) Parser() Parser {
	return swapEventParser
}

func (c *counter) Snapshot() ([]Event, error) {
	return []Event{swapEvent{0, c.Get()}}, nil
}

func (c *counter) Inc() (inc int, err error) {
	var success bool
	for {
		cur := c.Get()
		inc = cur + 1
		success, err = c.Swap(cur, inc)
		if err != nil || success {
			return
		}
	}
}

func (c *counter) Dec() (dec int, err error) {
	var success bool
	for {
		cur := c.Get()
		dec = cur - 1
		success, err = c.Swap(cur, dec)
		if err != nil || success {
			return
		}
	}
}

func (c *counter) Swap(e int, a int) (bool, error) {
	req := newSwapRequest(e, a)
	select {
	case <-c.closed:
		return false, ClosedError
	case c.swaps <- req:
		select {
		case <-c.closed:
			return false, ClosedError
		case r := <-req.ack:
			return r.success, r.err
		}
	}
}

func (c *counter) Run(log MachineLog) {
	// Committer routine
	go func() {
		defer log.Close()

		l, err := log.Listen()
		if err != nil {
			return
		}
		defer l.Close()


		for {
			select {
			case <-c.closed:
				return
			case <-l.Closed():
				return
			case i := <-l.Items():
				c.handleCommmit(i)
			}
		}
	}()

	// Request routine
	go func() {
		for {
			select {
			case <-c.closed:
				return
			case u := <-c.swaps:
				c.handleSwapRequest(log, u)
			}
		}
	}()
}

func (c *counter) handleCommmit(i LogItem) {
	swap := i.Event.(swapEvent)
	c.updateVal(func(cur counterValue) (val counterValue) {
		if cur.index != i.Index-1 {
			panic("Out of order item")
		}

		if cur.Next != swap.Prev {
			val = counterValue{cur.swapEvent, i.Index}
		} else {
			val = counterValue{swap, i.Index}
		}

		listeners := c.listeners()
		for _, l := range listeners {
			select {
			case <-c.closed:
				return
			case <-l.closed:
				return
			case l.ch <- val:
			}
		}
		return
	})
}

func (c *counter) handleSwapRequest(log MachineLog, u *swapRequest) {
	c.swapPool.Submit(func() {
		listener, err := c.Listen()
		if err != nil {
			u.reply(false, err)
		}
		defer listener.Close()

		item, err := log.Append(u.swapEvent)
		if err != nil {
			u.reply(false, err)
			return
		}

		var val counterValue
		for {
			select {
			case <-listener.Closed():
				u.reply(false, ClosedError)
				return
			case v := <-listener.Values():
				if v.index > item.Index {
					u.reply(false, errors.Wrapf(EventError, "Missed committed value [%v]", item.Index))
					return
				}

				if v.index == item.Index {
					val = v
				}
			}
		}

		u.reply(val.swapEvent == u.swapEvent, nil)
	})
}

func (c *counter) ensureOpen() error {
	select {
	case <-c.closed:
		return ClosedError
	default:
		return nil
	}
}

func (c *counter) listeners() (ret []*counterListener) {
	all := c.subs.All()
	ret = make([]*counterListener, 0, len(all))
	for k, _ := range all {
		ret = append(ret, k.(*counterListener))
	}
	return
}

func (c *counter) Listen() (*counterListener, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}

	ret := newCounterListener(c)
	c.subs.Put(ret, struct{}{})
	return ret, nil
}

func (c *counter) val() counterValue {
	c.valueLock.Lock()
	defer c.valueLock.Unlock()
	return c.value
}

func (c *counter) updateVal(fn func(counterValue) counterValue) {
	c.valueLock.Lock()
	c.value = fn(c.value)
	c.valueLock.Unlock()
}

type counterListener struct {
	counter *counter
	ch      chan counterValue
	closed  chan struct{}
	closer  chan struct{}
}

func newCounterListener(counter *counter) *counterListener {
	return &counterListener{counter, make(chan counterValue, 8), make(chan struct{}), make(chan struct{}, 1)}
}

func (l *counterListener) Closed() <-chan struct{} {
	return l.closed
}

func (l *counterListener) Values() <-chan counterValue {
	return l.ch
}

func (l *counterListener) Close() error {
	select {
	case <-l.closed:
		return ClosedError
	case l.closer <- struct{}{}:
	}

	l.counter.subs.Remove(l)
	close(l.closed)
	return nil
}

package kayak

import (
	"io"
	"sync"

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
	valueLock *sync.Cond
	commits   chan counterValue
	swaps     chan *swapRequest
	swapPool  concurrent.WorkPool
	closed    chan struct{}
	closer    chan struct{}
}

func NewReplicatedCounter(ctx common.Context, port int, peers []string) (ReplicatedCounter, error) {
	counter := &counter{
		ctx:       ctx,
		valueLock: &sync.Cond{L: &sync.Mutex{}},
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
	go func() {
		defer log.Close()
		for {
			select {
			case <-c.closed:
				return
			case <-log.Closed():
				return
			case u := <-c.swaps:
				c.handleSwapRequest(log, u)
			}
		}
	}()
}

func (c *counter) Commit(i LogItem) {
	swap := i.Event.(swapEvent)
	c.updateVal(func(cur counterValue) counterValue {
		if cur.index != i.Index-1 {
			panic("Out of order item")
		}

		if cur.Next != swap.Prev {
			return counterValue{cur.swapEvent, i.Index}
		} else {
			return counterValue{swap, i.Index}
		}
	})
}

func (c *counter) handleSwapRequest(log MachineLog, u *swapRequest) {
	c.swapPool.Submit(func() {

		item, err := log.Append(u.swapEvent)
		if err != nil {
			u.reply(false, err)
			return
		}

		// FIXME:  This currently has a race condition where we may
		// miss the value we're interested in.  If we do, then
		// we would return an unsuccessful response, when in fact,
		// we just don't know.
		//
		// Make a listener that watches the counter state (committed, swap)
		// in realtime and make sure to take the listener before append!
		val, err := c.sync(item.Index)
		if err != nil {
			u.reply(false, err)
			return
		}

		u.reply(val.swapEvent == u.swapEvent, nil)
	})
}

func (c *counter) sync(index int) (val counterValue, err error) {
	c.valueLock.L.Lock()
	defer c.valueLock.L.Unlock()
	for val = c.val(); val.index < index; val = c.val() {
		c.valueLock.Wait()
		select {
		default:
			continue
		case <-c.closed:
			return val, ClosedError
		}
	}
	return
}

func (c *counter) val() counterValue {
	c.valueLock.L.Lock()
	defer c.valueLock.L.Unlock()
	return c.value
}

func (c *counter) updateVal(fn func(counterValue) counterValue) {
	c.valueLock.L.Lock()
	c.value = fn(c.value)
	c.valueLock.L.Unlock()
	c.valueLock.Broadcast()
}

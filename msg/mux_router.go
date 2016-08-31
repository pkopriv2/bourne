package msg

import (
	"sync"

	"github.com/pkopriv2/bourne/msg"
)

// A channel router accepts packets and routes them to
// to their target destinations.
//
type MuxRouter struct {

	// the map of all channels.
	cache *RoutingTable

	// the input chan (BUFFERED: ROUTER_BUF_IN_SIZE)
	in chan Packet

	// the output chan
	out chan Packet

	// the routers lock (internal only!)
	lock sync.RWMutex

	// a flag indicating that the router is closed.
	closed bool

	// worker
	worker sync.WaitGroup
}

// Creates and returns a new channel.
//
func NewMuxRouter(cache *RoutingTable, out chan Packet) *MuxRouter {

	// create the router
	router := &MuxRouter{
		cache: cache,
		in:    make(chan Packet, ROUTER_BUF_IN_SIZE),
		out:   out}

	// create the in channel routine
	go func(router *MuxRouter) {
		router.worker.Add(1)
		defer router.worker.Done()

		// just loop and read
		for {
			// p, ok := <-router.in
			// if !ok {
			// // channel closed
			// return
			// }
			//
			// channel, err := router.getChannel(&p)
			// if channel == nil || err != nil {
			// router.out <- *NewErrorPacket(&p, CHANNEL_REFUSED_ERROR)
			// return
			// }
			//
			// channel.Send(p)
		}
	}(router)

	// create the worker
	return router
}

// Closes the channel.  Returns an error the if the
// channel is already closed.
//
func (m *MuxRouter) Close() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	close(m.in)
	m.closed = true
	return nil
}

func (m *MuxRouter) SendIn(p *msg.Packet) error {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.closed {
		return CHANNEL_CLOSED_ERROR
	}

	self.in <- *p
	return nil
}

func (m *MuxRouter) SendOut(p *msg.Packet) error {
	panic("not implemented")
}

// split out for more granular locking.
func (self *MuxRouter) getChannel(p *Packet) (*PacketProcessor, error) {
	self.lock.RLock()
	defer self.lock.RUnlock()

	if self.closed {
		return nil, CHANNEL_CLOSED_ERROR
	}

	return nil, nil
	// return self.cache.Get[ChannelAddress{p.dstEntityId, p.dstChannelId}], nil
}

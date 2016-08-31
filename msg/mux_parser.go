package msg

import "sync"

// A channel router accepts packets and routes them to
// to their target destinations.
//
type MuxParser struct {

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
func NewMuxParser(cache *RoutingTable, out chan Packet) *MuxParser {

	// create the router
	router := &MuxParser{
		cache: cache,
		in:    make(chan Packet, ROUTER_BUF_IN_SIZE),
		out:   out}

	// create the router routine
	go func(router *MuxParser) {
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

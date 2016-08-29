package msg

import "sync"


// A channel router accepts packets and routes them to
// to their target destinations.
//
type ChannelRouter struct {

    // the map of all channels.
    cache *ChannelCache

    // the input chan
    in chan Packet

    // the output chan
    out chan Packet

    // the routers lock (internal only!)
    lock sync.RWMutex

    // a flag indicating that the router is closed.
    closed bool
}

// Creates and returns a new channel.
//
func NewChannelRouter(cache *ChannelCache, out chan Packet) *ChannelRouter {

    // create the router
    router := &ChannelRouter {
        cache : cache,
        in    : make(chan Packet, ROUTER_BUF_IN_SIZE),
        out   : out }

    // create the worker
    return router
}

// Closes the channel.  Returns an error the if the
// channel is already closed.
//
func (self *ChannelRouter) Close() error {
    self.lock.Lock(); defer self.lock.Unlock()

    close(self.in)
    self.closed = true
    return nil
}

// Sends a packet to the channel stream.
//
func (self *ChannelRouter) Send(p *Packet) error {
    packet, ok := <- self.in
    if ! ok {
        return CHANNEL_CLOSED_ERROR
    }

    channel := self.getChannel(p);
    if channel == nil {
        self.out <- *NewErrorPacket(&packet, CHANNEL_REFUSED_ERROR)
        return nil
    }

    channel.Send(&packet)
    return nil
}

// split out for more granular locking.
func (self *ChannelRouter) getChannel(p *Packet) RoutableChannel {
    self.lock.RLock(); defer self.lock.RUnlock()
    if self.closed {
        return CHANNEL_CLOSED_ERROR
    }

    return self.cache.Get[ChannelAddress{p.dstEntityId, p.dstChannelId}]
}

package msg

import (
	"errors"
	"sync"
)

// Listeners listen on the range [0,255]
var CHANNEL_LISTEN_MAX_ID uint16    = 255
var CHANNEL_LISTEN_MIN_ID uint16    = 0

// Each listener can buffer n packets
var CHANNEL_LISTEN_BUF_SIZE uint    = 1024

//
var CHANNEL_LISTEN_INV_ID_ERR error = errors.New("Invalid listener id.")

// A listening channel is a simple channel awaiting new channel requests.
//
// *This object is thread safe.*
//
type ChannelListener struct {

	// the local channel address
	local *ChannelAddress

	// the channel cache (shared amongst all channels)
	cache *ChannelCache

	// the pool of ids (shared amongst all channels)
	ids *IdPool

	// the buffered "in" channel (owned by this channel)
	in chan Packet

	// the buffered "out" channel (shared amongst all channels)
	out chan Packet

	// the lock on this channel
	lock sync.RWMutex

	// a flag indicating that the channel is closed (updates/reads must be synchronized)
	closed bool
}

// Creates and returns a new listening channel.  This has the side effect of adding the
// channel to the channel cache, which means that it immediately available to have
// packets routed to it.
//
func NewChannelListener(local *ChannelAddress, cache *ChannelCache, ids *IdPool, out chan Packet) (*ChannelListener, error) {
	if local == nil  {
		panic("local address must not be nil")
	}

	if cache == nil  {
		panic("Cache must not be nil")
	}

	if ids == nil  {
		panic("Cache must not be nil")
	}

	if local.channelId > CHANNEL_LISTEN_MAX_ID {
		return nil, CHANNEL_LISTEN_INV_ID_ERR
	}

	// buffered input chan
	in := make(chan Packet, CHANNEL_LISTEN_BUF_SIZE)

	// create the channel
	channel := &ChannelListener{
		local: local,
		cache: cache,
		ids:   ids,
		out:   out,
		in:    in}

	// add it to the channel pool (i.e. make it available for routing)
	if err := cache.Add(*local, channel); err != nil {
		return nil, err
	}

	// finally, return control to the caller
	return channel, nil
}

func (self *ChannelListener) Accept() (Channel, error) {
	for {
		packet, ok := <-self.in
		if !ok {
			return nil, CHANNEL_CLOSED_ERROR
		}

		if channel, err := self.tryAccept(&packet); channel != nil || err != nil {
			return channel, err
		}
	}
}

// split out for a more granular locking strategy
func (self *ChannelListener) tryAccept(p *Packet) (Channel, error) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.closed {
		return nil, CHANNEL_CLOSED_ERROR
	}

	// ensure this packet is at the right destination. if not, return it!
	if p.dstEntityId != self.local.entityId || p.dstChannelId != self.local.channelId {
		// self.out <- *NewReturnPacket(p, ERR_FLAG, []uint8(CHANNEL_REFUSED_ERROR.Error()))
		return nil, nil
	}

	// create the channel address
	lChannelId, err := self.ids.Take()
	if(err != nil) {
		// TODO: return error packet.
		// self.out<- NewReturnPacket
		return nil, err
	}

	lAddr := ChannelAddress{p.dstEntityId, lChannelId}
	rAddr := ChannelAddress{p.srcEntityId, p.srcChannelId}

	return NewChannelActive(lAddr, rAddr, func(opts *ChannelOptions) {

		// make the channel routable.
		opts.OnInit = func(c *ChannelActive) error {
			return self.cache.Add(c.local, c)
		}

		// route to the main output
		opts.OnData = func(p *Packet) error {
			self.out<-*p
			return nil
		}

		// return the resources.
		opts.OnClose = func(c *ChannelActive) error {
			defer self.ids.Return(c.local.channelId)
			defer self.cache.Remove(c.local)
			return nil
		}
	});
}

// Sends a packet to the channel stream.
//
func (self *ChannelListener) Send(p *Packet) error {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.closed {
		return CHANNEL_CLOSED_ERROR
	}

	self.in <- *p
	return nil
}

// Closes the channel.  Returns an error the if the
// channel is already closed.
//
func (self *ChannelListener) Close() error {
	self.lock.Lock()
	defer self.lock.Unlock()
	if self.closed {
		return CHANNEL_CLOSED_ERROR
	}

	// stop routing to this channel.
	self.cache.Remove(*self.local)

	// close the buffer
	close(self.in)

	// finally, mark it closed
	self.closed = true
	return nil
}

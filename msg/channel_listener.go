package msg

import (
	"sync"
)

// A listening channel is a simple channel awaiting new channel
// requests.
//
// *This object is thread safe.*
//
type ChannelListener struct {

	// the local channel address
	local ChannelAddress

	// the channel cache (shared amongst all channels)
	cache *ChannelCache

	// the pool of ids (shared amongst all channels)
	ids *IdPool

	// the buffered "input" channel (owned by this channel)
	in chan Packet

	// the buffered "output" channel (shared amongst all channels)
	out chan Packet

	// the lock on this channel
	lock sync.RWMutex

	// a flag indicating that the channel is closed.
	closed bool
}

// Creates and returns a new listening channel.
//
func NewChannelListener(local ChannelAddress, cache *ChannelCache, ids *IdPool, out chan Packet) *ChannelListener {

	// buffered input chan
	in := make(chan Packet, CHANNEL_BUF_IN_SIZE)

	// create the channel
	channel := &ChannelListener{
		local: local,
		cache: cache,
		ids:   ids,
		out:   out,
		in:    in}

	// start routing to the channel
	cache.Add(local, channel)

	// finally, return control to the caller
	return channel
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

func (self *ChannelListener) tryAccept(p *Packet) (Channel, error) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.closed {
		return nil, CHANNEL_CLOSED_ERROR
	}

	// grab an id from the pool (this currently drops the packet)
	channelId, err := self.ids.Take()
	if err != nil {
		self.out <- *NewErrorPacket(p, CHANNEL_REFUSED_ERROR)
		return nil, nil
	}

	// ensure this packet is at the right destination. if not, return it!
	if p.dstEntityId != self.local.entityId || p.dstChannelId != self.local.channelId {
		self.out <- *NewErrorPacket(p, CHANNEL_REFUSED_ERROR)
		return nil, nil
	}

	// derive a new local address
	lAddr := ChannelAddress{p.dstEntityId, channelId}

	// pull the remote address from the packet.
	rAddr := ChannelAddress{p.srcEntityId, p.srcChannelId}

	// create and return the active channel
	return NewActiveChannel(lAddr, rAddr, self.cache, self.ids, self.out), nil
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
	self.cache.Remove(self.local)

	// close the buffer
	close(self.in)

	// finally, mark it closed
	self.closed = true
	return nil
}

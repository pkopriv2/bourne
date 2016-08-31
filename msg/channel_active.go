package msg

import (
	"io"
	"sync"
)

// Each channel is able to buffer up to a certain number
// of packets on its incoming stream.
var CHANNEL_ACTIVE_BUF_SIZE uint = 1024

// An active channel represents one side of a conversation between two entities
//
// *This object is thread safe.*
//
type ChannelActive struct {

	// the local channel address
	local ChannelAddress

	// the remote channel address
	remote ChannelAddress

	// the cache tracking this channel
	cache *ChannelCache

	// the pool of ids.
	ids *IdPool

	// the packet->stream reader
	reader io.Reader

	// the stream->packet writer
	writer io.Writer

	// the buffered "input" channel (owned by this channel)
	in chan Packet

	// a flag indicating that the channel is closed. (atomic bool)
	closed bool

	// the channel's lock
	lock sync.RWMutex
}

// Creates and returns a new channel.  This method has no side-effects.
//
func NewActiveChannel(srcEntityId uint32, r ChannelAddress, cache *ChannelCache, ids *IdPool, out chan Packet) (*ChannelActive, error) {
	// generate a new id.
	channelId, err := ids.Take()
	if err != nil {
		return nil, CHANNEL_REFUSED_ERROR
	}

	// derive a new local address
	l := ChannelAddress{srcEntityId, channelId}

	// buffered input chan (for now just passing right to consumer.)
	in := make(chan Packet, CHANNEL_ACTIVE_BUF_SIZE)

	// io abstractions
	reader := NewPacketReader(in)
	writer := NewPacketWriter(out, l.entityId, l.channelId, r.entityId, r.channelId)

	// create the router
	channel := &ChannelActive{
		local:  l,
		remote: r,
		cache:  cache,
		ids:    ids,
		in:     out,
		reader: reader,
		writer: writer}

	// add it to the channel pool (i.e. make it available for routing)
	if err := cache.Add(l, channel); err != nil {
		return nil, err
	}

	// finally, return it.
	return channel, nil
}

// Returns the local address of this channel
//
func (self *ChannelActive) LocalAddr() ChannelAddress {
	return self.local
}

// Returns the remote address of this channel
//
func (self *ChannelActive) RemoteAddr() ChannelAddress {
	return self.remote
}

// Reads data from the channel.  Blocks if data isn't available.
//
func (self *ChannelActive) Read(buf []byte) (int, error) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.closed {
		return 0, CHANNEL_CLOSED_ERROR
	}

	return self.reader.Read(buf)
}

// Writes data to the channel.  Blocks if the underlying buffer is full.
//
func (self *ChannelActive) Write(data []byte) (int, error) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.closed {
		return 0, CHANNEL_CLOSED_ERROR
	}

	return self.writer.Write(data)
}

// Sends a packet to the channel stream.
//
func (self *ChannelActive) Send(p *Packet) error {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.closed {
		return CHANNEL_CLOSED_ERROR
	}

	self.in <- *p
	return nil
}

// Closes the channel.  Returns an error if the
// channel is already closed.
//
func (self *ChannelActive) Close() error {
	self.lock.Lock()
	defer self.lock.Unlock()
	if self.closed {
		return CHANNEL_CLOSED_ERROR
	}

	// stop routing.
	self.cache.Remove(self.local)

	// return this channel's id to the pool
	self.ids.Return(self.local.channelId)

	// close all 'owned' io objects
	close(self.in)

	self.reader = nil
	self.writer = nil
	self.closed = true

	return nil
}

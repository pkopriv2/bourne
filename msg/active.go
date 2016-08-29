package msg

import (
    "io"
    "sync"
)

// An active channel represents one side of a conversation between two entities
//
// *This object is thread safe.*
//
type ActiveChannel struct {
    // the local channel address
    local  ChannelAddress

    // the remote channel address
    remote ChannelAddress

    // the cache tracking this channel
    cache  *ChannelCache

    // the pool of ids.
    ids    *IdPool

    // the packet->stream reader
    reader io.Reader

    // the stream->packet writer
    writer io.Writer

    // the buffered "input" channel (owned by this channel)
    in     chan Packet

    // a flag indicating that the channel is closed. (atomic bool)
    closed bool

    // the channel's lock
    lock   sync.RWMutex
}

// Creates and returns a new channel.  This has the side effect of
//
func NewActiveChannel(l ChannelAddress, r ChannelAddress, cache *ChannelCache, ids *IdPool, out chan Packet) *ActiveChannel {
    // buffered input chan
    in := make(chan Packet, CHANNEL_BUF_IN_SIZE)

    // io abstractions
    reader := NewPacketReader(in)
    writer := NewPacketWriter(out, l.entityId, l.channelId, r.entityId, r.channelId)

    // create the router
    channel := &ActiveChannel{
        local  : l,
        remote : r,
        cache  : cache,
        ids    : ids,
        in     : out,
        reader : reader,
        writer : writer }

    // start routing to the channel
    cache.Add(channel)

    // finally, return it.
    return channel
}

// Reads data from the channel.  Blocks if data isn't available.
//
func (self *ActiveChannel) Read(buf []byte) (int, error) {
    self.lock.RLock(); defer self.lock.RUnlock();
    if self.closed {
        return 0, CHANNEL_CLOSED_ERROR
    }

    return self.reader.Read(buf);
}

// Writes data to the channel.  Blocks if the underlying buffer is full.
//
func (self *ActiveChannel) Write(data []byte) (int, error) {
    self.lock.RLock(); defer self.lock.RUnlock();
    if self.closed() {
        return 0, CHANNEL_CLOSED_ERROR
    }

    return self.writer.Write(data);
}

// Sends a packet to the channel stream.
//
func (self *ActiveChannel) Send(p *Packet) error {
    self.lock.RLock(); defer self.lock.RUnlock();
    if self.closed {
        return 0, CHANNEL_CLOSED_ERROR
    }

    self.in <- *p
    return nil
}

// Closes the channel.  Returns an error if the
// channel is already closed.
//
func (self *ActiveChannel) Close() error {
    self.lock.Lock(); defer self.lock.Unlock();
    if self.closed {
        return CHANNEL_CLOSED_ERROR
    }

    // stop routing.
    self.cache.Remove(self)

    // return this channel's id to the pool
    self.ids.Return(self.local.channelId)

    // close all 'owned' io objects
    close(self.in)

    self.reader = nil
    self.writer = nil
    self.closed = true

    return nil
}

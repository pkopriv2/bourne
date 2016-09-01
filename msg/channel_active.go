package msg

import (
	"io"
	"sync"
	"sync/atomic"
)

var CHANNEL_RESEND_BUF_SIZE uint = 1024
var CHANNEL_RECV_BUF_SIZE uint   = 1024
var CHANNEL_SEND_BUF_SIZE uint   = 1024

// An active channel represents one side of a conversation between two entities.
//
// The majority of packet stream logic is handled here. The fundamental laws of
// channels are as follows:
//
//   * A channel reprents a full-duplex stream abstraction.  In other words,
//     there are independent input and output streams.  Read again: INDEPENDENT!
//
//   * A channel may be thought of as two output streams.  One output on each
//     side of the conversation.
//
//   * Senders are responsible for the reliability of their streams.
//
//   * Each sender must never forget a message until it has been acknowledged.
//
//   * In the event that its "memory" is exhausted, a sender can force an acknowledgement
//     from a receiver.  The receiver MUST respond.  The sender will resume transmission
//     from the point.
//
//   * A receiver will only accept a packet if its sequence is exactly ONE greater
//     than the last value seen.  All packets are dropped that do not meet this
//     criteria. (WE WILL TRACK DROPPED PACKETS AND MAKE A MORE INFORMED DECISION)
//
//  Similar to TCP, this protocol ensures reliable, in-order delivery of data.
//  However, unlike TCP, this does attempt to solve the following problems:
//
//     * Flow control (we don't care if we overwhelm a recipient.  we will inherit aspects of this from TCP)
//     * Congestion control (we don't care if a network is overwhelmed)
//     * Message integrity (we won't do any integrity checking)
//
// *This object is thread safe.*
//
type ChannelActive struct {

	// the local channel address
	local ChannelAddress

	// the remote channel address
	remote ChannelAddress

	// the cache tracking this channel (used during initalization and closing)
	cache *ChannelCache

	// the pool of ids. (used during initalization and closing)
	ids *IdPool

	// the packet->[]byte reader
	reader io.Reader

	// the []byte->[]byte writer
	writer io.Writer

	// The reader channels internal ---> reader --> consumer
	readerIn  chan Packet
	readerOut chan Packet

	// The writer channels consumer ---> writer --> internal
	writerIn  chan []byte
	writerOut chan Packet

	// the maximum packet sequence that has been sent vs acked. (updated via atomic swap)
	sendSeq uint32
	sendAck uint32
	sendMem [1024]Packet // len(sendMem) > sendSeq - sendAck

	// the maximum *valid* packet sequence that has been received. (updated via atomic swap)
	recvSeq uint32
	recvDropped uint32 // track how often packages are dropped.

	// the workers wait
	workers sync.WaitGroup

	// a flag indicating that the channel is closed. (synchronized on lock)
	closed bool

	// the channel's lock
	lock sync.RWMutex
}

// Creates and returns a new channel.  This method has no side-effects.
//
func NewActiveChannel(srcEntityId uint32, r ChannelAddress, cache *ChannelCache, ids *IdPool, out chan Packet) (*ChannelActive, error) {
	// generate a new local channel id.
	channelId, err := ids.Take()
	if err != nil {
		return nil, CHANNEL_REFUSED_ERROR
	}

	// derive a new local address
	l := ChannelAddress{srcEntityId, channelId}

	// consumer reader abstractions
	readerOut := make(chan Packet)
	reader := NewPacketReader(readerOut)

	// consumer writer abstractions
	writerIn := make(chan []byte)
	writer := NewPacketWriter(writerIn)

	// internal abstractions
	var readerIn = make(chan Packet, CHANNEL_ACTIVE_BUF_SIZE)
	var writerOut = out

	// create the router
	c := &ChannelActive{
		local:     l,
		remote:    r,
		cache:     cache,
		ids:       ids,
		reader:    reader,
		writer:    writer,
		readerIn:  readerIn,
		readerOut: readerOut,
		writerIn:  writerIn,
		writerOut: writerOut,
		sendSeq: 0,
		sendAck: 0,
		recvSeq: 0}

	// kick off the reader
	c.workers.Add(1)
	go func(c *ChannelActive) {
		defer c.workers.Done()
		for {
			p, ok := <-c.readerIn
			if !ok {
				return // channel closed
			}

			// HANDLE: FIN FLAG (close shop)
			if p.ctrls&FIN_FLAG > 0 {
				c.writerOut<-Packet{}
			}

			// HANDLE: SYN FLAG (updates the internal recvSeq counter)
			if p.ctrls&SYN_FLAG > 0 {
				var recvSeq = c.recvSeq

				// HANDLE: packet in the past (we should just drop it?)
				if p.seq <= recvSeq {
					continue
				}

				// HANDLE: packet in the future
				// Okay,
				if p.seq > recvSeq+1 {
					continue
				}

				atomic.SwapUint32(&c.recvSeq, recvSeq+1)
			}

			// HANDLE: ACK FLAG (updates the internal ack counter)
			if p.ctrls&ACK_FLAG > 0 {
				if p.ack > c.sendAck {
					atomic.SwapUint32(&c.sendAck, p.ack)
				}
			}

			// send it on.
			c.readerOut <- p
		}
	}(c)

	// kick off the writer
	go func(c *ChannelActive) {
		for {
			// bytes, ok := <-writerIn
			// if ! ok {
			// return // channel closed
			// }
		}
	}(c)

	// kick off the heartbeat thread
	go func(c *ChannelActive) {
		for {
			// p := <-readerIn
		}
	}(c)

	// add it to the channel pool (i.e. make it available for routing)
	if err := cache.Add(l, c); err != nil {
		return nil, err
	}

	// finally, return it.
	return c, nil
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

	self.readerIn <- *p
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
	close(self.readerIn)
	close(self.readerOut)
	close(self.writerIn)

	// finally, wait for the workers to be done.
	self.workers.Wait()
	self.closed = true
	return nil
}

package msg

import (
	"io"
	"sync"
	"sync/atomic"
)

var CHANNEL_RESEND_BUF_SIZE uint = 1024
var CHANNEL_RECV_BUF_SIZE uint = 1024
var CHANNEL_SEND_BUF_SIZE uint = 1024

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

	// The receiver channels internal ---> receiver --> consumer
	receiverIn  chan Packet
	receiverOut chan Packet
	receiverRaw io.Reader // consumer reader

	// The sender channels consumer ---> sender --> internal
	senderRaw io.Writer // consumer writer
	senderIn  chan []byte
	senderOut chan Packet

	// the maximum packet sequence that has been sent vs acked. (updated via atomic swap)
	sendSeq     uint32
	sendAck     uint32
	sendMem     [1024]Packet // len(sendMem) > sendSeq - sendAck
	sendTimeout uint         // amount in millis until a channel is considered irrecoverable.

	// the maximum *valid* packet sequence that has been received. (updated via atomic swap)
	recvSeq     uint32
	recvDropped uint32 // track how often packets are dropped.

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

	// consumer receiver abstractions
	receiverOut := make(chan Packet)
	receiver := NewPacketReader(receiverOut)

	// consumer sender abstractions
	senderIn := make(chan []byte)
	sender := NewPacketWriter(senderIn)

	// internal abstractions
	var receiverIn = make(chan Packet, CHANNEL_RECV_BUF_SIZE)
	var senderOut = out

	// create the router
	c := &ChannelActive{
		local:       l,
		remote:      r,
		cache:       cache,
		ids:         ids,
		receiverIn:  receiverIn,
		receiverOut: receiverOut,
		receiverRaw: receiver,
		senderRaw:   sender,
		senderIn:    senderIn,
		senderOut:   senderOut,
		sendSeq:     0,
		sendAck:     0,
		recvSeq:     0}

	// kick off the receiver
	c.workers.Add(1)
	go func(c *ChannelActive) {
		defer c.workers.Done()
		for {
			p, ok := <-c.receiverIn
			if !ok {
				return // channel closed
			}

			// HANDLE: FRC FLAG (respond with ack)
			if p.ctrls&FRC_FLAG > 0 {
				// respond with ACK message
				// c.senderOut<-Packet{}
			}

			// HANDLE: FIN FLAG (close shop)
			if p.ctrls&FIN_FLAG > 0 {
				// respond with ACK,FIN message
				// c.senderOut<-Packet{}
			}

			// HANDLE: ACK FLAG (updates send ack)
			if p.ctrls&ACK_FLAG > 0 {
				if p.ack > c.sendAck {
					atomic.SwapUint32(&c.sendAck, p.ack)
				}
			}

			// HANDLE: SEQ_FLAG (signifies that data is coming!)
			if p.ctrls&SEQ_FLAG > 0 {
				// Per channel laws, drop any packet not at proper sequence
				var recvSeq = c.recvSeq
				if p.seq <= recvSeq {
					continue
				}

				if p.seq > recvSeq+1 {
					continue
				}

				atomic.SwapUint32(&c.recvSeq, recvSeq+1)
			}

			// send it on.
			c.receiverOut <- p
		}
	}(c)

	// kick off the sender
	go func(c *ChannelActive) {
		for {
			// bytes, ok := <-senderIn
			// if ! ok {
			// return // channel closed
			// }
		}
	}(c)

	// kick off the heartbeat thread
	go func(c *ChannelActive) {
		for {
			// p := <-receiverIn
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

	return self.receiverRaw.Read(buf)
}

// Writes data to the channel.  Blocks if the underlying buffer is full.
//
func (self *ChannelActive) Write(data []byte) (int, error) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.closed {
		return 0, CHANNEL_CLOSED_ERROR
	}

	return self.senderRaw.Write(data)
}

// Sends a packet to the channel stream.
//
func (self *ChannelActive) Send(p *Packet) error {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.closed {
		return CHANNEL_CLOSED_ERROR
	}

	self.receiverIn <- *p
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
	close(self.receiverIn)
	close(self.receiverOut)
	close(self.senderIn)

	// finally, wait for the workers to be done.
	self.workers.Wait()
	self.closed = true
	return nil
}

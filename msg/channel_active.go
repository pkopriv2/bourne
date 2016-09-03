package msg

import (
	"errors"
	"sync"

	"github.com/Workiva/go-datastructures/queue"
)

var CHANNEL_RECV_BUF_IN_SIZE uint64 = 1024   // 1024 packet channel
var CHANNEL_RECV_BUF_OUT_SIZE uint64 = 32768 // 32k byte channel
var CHANNEL_SEND_BUF_SIZE uint64 = 32768

var ERR_CHANNEL_INVALID_ACK = errors.New("ACK:INVALID")

// An active channel represents one side of a conversation between two entities.
//
// The majority of packet stream logic is handled here. The fundamental laws of
// channels are as follows:
//
//   * A channel reprents a full-duplex stream abstraction.  In other words,
//     there are independent input and output streams.  Read again: INDEPENDENT!
//
//   * A channel may be thought of as two send streams, with one sender on each
//     side of the conversation.
//
//   * Each sender is responsible for the reliability his data stream.
//
//   * Each sender must never forget a message until it has been acknowledged.
//
//   * In the event that its "memory" is exhausted, a sender can force an acknowledgement
//     from a receiver.  The receiver MUST respond.  The send will resume transmission
//     from the point.
//
//   * A receiver will only accept a data from a packet if its sequence is exactly ONE
//     greater than the last value seen.  All packets are dropped that do not meet this
//     criteria. (WE WILL TRACK DROPPED PACKETS AND MAKE A MORE INFORMED DECISION LATER)
//
//  Similar to TCP, this protocol ensures reliable, in-order delivery of data.
//  This also allows sessions to be resumed if errors occur. However, unlike
//  TCP, this does NOT attempt to solve the following problems:
//
//     * Flow control (we don't care if we overwhelm a recipient.  we will inherit aspects of this from TCP)
//     * Congestion control (we don't care if a network is overwhelmed)
//     * Message integrity (we won't do any integrity checking!)
//
//  Send Data Flow:
//
//     <consumer> ---> [ ring buffer ] ---> sender ---> out
//
//  Receiver
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

	// The recv channels internal ---> recv --> consumer
	recvIn  chan Packet
	recvOut *queue.RingBuffer // T: byte
	recvSeq uint32            // the maximum *valid* byte sequence that has been received. (updated via atomic swap)

	// The send channels consumer ---> send --> internal
	sendIn  *SendBuffer
	sendOut chan Packet

	// the workers wait
	workers sync.WaitGroup

	// a flag indicating that the channel is closed. (synchronized on lock)
	closed bool

	// the channel's lock
	lock sync.RWMutex
}

// Creates and returns a new channel.  This method has the following side effects:
//
//   * Pulls an id from the id pool
//   * Adds an entry to the channel cache (making it *routable*)
//
func NewActiveChannel(srcEntityId uint32, r ChannelAddress, cache *ChannelCache, ids *IdPool, out chan Packet) (*ChannelActive, error) {
	return nil, nil
	// generate a new local channel id.
	// channelId, err := ids.Take()
	// if err != nil {
	// return nil, err
	// }

	// derive a new local address
	// l := ChannelAddress{srcEntityId, channelId}

	// receive queues
	// var recvIn = queue.NewRingBuffer(CHANNEL_RECV_BUF_IN_SIZE)
	// var recvOut = queue.NewRingBuffer(CHANNEL_RECV_BUF_OUT_SIZE)
	//
	// // send queues
	// var sendIn = queue.NewRingBuffer(CHANNEL_SEND_BUF_SIZE)
	// var sendOut = out
	//
	// // create the router
	// c := &ChannelActive{
	// local:   l,
	// remote:  r,
	// cache:   cache,
	// ids:     ids,
	// recvIn:  recvIn,
	// recvOut: recvOut,
	// sendIn:  sendIn,
	// sendOut: sendOut,
	// sendSeq: 0,
	// sendAck: 0,
	// recvSeq: 0}
	//
	// // kick off the recv
	// c.workers.Add(1)
	// go func(c *ChannelActive) {
	// defer c.workers.Done()
	// for {
	// p, ok := <-c.recvIn
	// if !ok {
	// return // channel closed
	// }
	//
	// // HANDLE: FRC FLAG (respond with ack)
	// if p.ctrls&FRC_FLAG > 0 {
	// // respond with ACK message
	// // c.sendOut<-Packet{}
	// }
	//
	// // HANDLE: FIN FLAG (close shop)
	// if p.ctrls&FIN_FLAG > 0 {
	// // respond with ACK,FIN message
	// // c.sendOut<-Packet{}
	// }
	//
	// // HANDLE: ACK FLAG (updates send ack)
	// if p.ctrls&ACK_FLAG > 0 {
	// if p.ack == c.sendAck {
	// // retransmit from ack.
	// }
	//
	// if p.ack > c.sendAck {
	// atomic.SwapUint32(&c.sendAck, p.ack)
	// }
	// }
	//
	// // HANDLE: SEQ_FLAG (signifies that data is coming!)
	// if p.ctrls&SEQ_FLAG > 0 {
	// // Per channel laws, drop any packet not at proper sequence
	// var recvSeq = c.recvSeq
	// if p.seq <= recvSeq {
	// continue
	// }
	//
	// if p.seq > recvSeq+1 {
	// continue
	// }
	//
	// atomic.SwapUint32(&c.recvSeq, recvSeq+1)
	// }
	//
	// // send it on.
	// c.recvOut <- p
	// }
	// }(c)
	//
	// // kick off the send
	// go func(c *ChannelActive) {
	// for {
	// // bytes, ok := <-sendIn
	// // if ! ok {
	// // return // channel closed
	// // }
	// }
	// }(c)
	//
	//
	// // add it to the channel pool (i.e. make it available for routing)
	// if err := cache.Add(l, c); err != nil {
	// return nil, err
	// }
	//
	// // finally, return it.
	// return c, nil
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

	return 0, nil
}

// Writes data to the channel.  Blocks if the underlying buffer is full.
//
func (self *ChannelActive) Write(data []byte) (int, error) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.closed {
		return 0, CHANNEL_CLOSED_ERROR
	}

	return 0, nil
}

// Sends a packet to the channel stream.
//
func (self *ChannelActive) Send(p *Packet) error {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.closed {
		return CHANNEL_CLOSED_ERROR
	}

	self.recvIn <- *p
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
	// close(self.recvIn)
	// close(self.recvOut)
	// close(self.sendIn)

	// finally, wait for the workers to be done.
	self.workers.Wait()
	self.closed = true
	return nil
}

// A mostly non-blocking thread-safe wrapping buffer
// This differs from a buffer
type SendBuffer struct {
	data []byte
	lock sync.RWMutex // just using simple, coarse lock
	seq  uint32
	ack  uint32
}

func NewSendBuffer(size uint32) *SendBuffer {
	// size = roundUp(size)
	return &SendBuffer{data: make([]byte, size)}
}

// Retrieves the position of the seq of the queue
func (s *SendBuffer) SeqPos() uint32 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.seq
}

// Retrieves the position of the ack of the queue
func (s *SendBuffer) AckPos() uint32 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.ack
}

// Retrieves all the data currently in the queue.  This copies the data
// in order to not affect
func (s *SendBuffer) Data() []byte {
	s.lock.RLock()
	defer s.lock.RUnlock()

	// returns nil if empty
	len := s.seq - s.ack
	if len == 0 {
		return []byte{}
	}

	ret := make([]byte, len)

	// grab their positions
	ackPos := s.ack
	seqPos := s.seq
	bufLen := uint32(cap(s.data))

	// just start copying until we get to seq
	var i uint32 = 0
	for ; i < len; i++ {
		pos := (ackPos + 1 + i) % bufLen

		ret[i] = s.data[pos]
		if pos == seqPos {
			break
		}
	}

	return ret
}

// Rather than copy a byte at a time, which is very lock intensive,
// we'll just add entire slices.  This function has no side effects
// if it is NOT successful.
//
//
func (s *SendBuffer) Add(val []byte) uint32 {
	s.lock.Lock()
	defer s.lock.Unlock()

	// get the new seq
	valLen := uint32(len(val))
	bufLen := uint32(len(s.data))

	// grab current positions
	ack := s.ack % bufLen
	seq := s.seq % bufLen

	// just copy til we can't copy anymore.
	var i uint32 = 0
	var pos uint32
	for ; i < valLen; i++ {
		pos = (seq + 1 + i)%bufLen
		s.data[pos] = val[i]
		if pos == ack {
			break
		}
	}

	s.seq = s.seq + i
	return i
}

func (s *SendBuffer) Ack(num uint32) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.ack+num > s.seq {
		return ERR_CHANNEL_INVALID_ACK
	}

	s.ack = s.ack + num
	return nil
}

func roundUp(v uint32) uint32 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return v
}

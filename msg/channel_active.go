package msg

import (
	"errors"
	"sync"
	"time"
)

// Defines how many packets will be buffered on receiving
var CHANNEL_RECV_IN_SIZE uint32 = 1024

// Defines how many bytes will be buffered prior to being consumed
var CHANNEL_RECV_OUT_SIZE uint32 = 32768

// Defines how many bytes will be buffered by consumer.
var CHANNEL_SEND_IN_SIZE uint32 = 32768

// Defines how many bytes are recalled before an ack is required.
var CHANNEL_SEND_BUF_SIZE uint32 = 524288 //  512K

// Defines how long to wait after each RingBuffer#tryWrite
var RINGBUF_WAIT = 10 * time.Millisecond

// returned when an an attempt to move the read position to an invalid value
var ERR_RINGBUF_READ_INVALID = errors.New("RINGBUF:READ_POSITION_INVALID")

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
//     greater than the write value seen.  All packets are dropped that do not meet this
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

	// receive buffers
	recvIn  chan Packet
	recvOut *RingBuffer
	recvSeq uint32 // maximum seq position that has been received

	// send buffers
	sendIn  *RingBuffer
	sendBuf *RingBuffer
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
	// generate a new local channel id.
	channelId, err := ids.Take()
	if err != nil {
		return nil, err
	}

	// derive a new local address
	l := ChannelAddress{srcEntityId, channelId}

	// receive queues
	recvIn := make(chan Packet, CHANNEL_RECV_IN_SIZE)
	recvOut := NewRingBuffer(CHANNEL_RECV_OUT_SIZE)

	// send queues
	sendIn := NewRingBuffer(CHANNEL_RECV_OUT_SIZE)
	sendBuf := NewRingBuffer(CHANNEL_SEND_BUF_SIZE)
	sendOut := out

	// create the channel
	c := &ChannelActive{
		local:   l,
		remote:  r,
		cache:   cache,
		ids:     ids,
		recvIn:  recvIn,
		recvOut: recvOut,
		sendIn:  sendIn,
		sendBuf: sendBuf,
		sendOut: sendOut}

	// kick off the recv
	c.workers.Add(1)
	go func(c *ChannelActive) {
		defer c.workers.Done()
		for {
			p, ok := <-c.recvIn
			if !ok {
				return // channel closed
			}

			// HANDLE: ACK FLAG (updates send ack)
			if p.ctrls&ACK_FLAG > 0 {
			}

			// HANDLE: SEQ_FLAG (signifies that data is coming!)
			if p.ctrls&SEQ_FLAG > 0 {
			}
		}
	}(c)

	// kick off the send
	go func(c *ChannelActive) {
		for {
			// bytes, ok := <-sendIn
			// if ! ok {
			// return // channel closed
			// }
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

	return self.recvOut.Read(buf)
}

// Writes data to the channel.  Blocks if the underlying buffer is full.
//
func (self *ChannelActive) Write(data []byte) (int, error) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.closed {
		return 0, CHANNEL_CLOSED_ERROR
	}

	return self.sendIn.Write(data)
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

// A very simple circular buffer designed to buffer bytes.
// This implementation differs from most others in the following
// ways:
//    * The value of the buffer may be safely accessed without
//      side effects.  See: #Data()
//    * Values may be dropped.
//
// This object is thread-safe.
//
type RingBuffer struct {
	data []byte
	lock sync.RWMutex // just using simple, coarse lock

	write uint32 // the write position in the buffer (writes start here)
	read  uint32 // the read position in the buffer (reads start here)
}

// Returns a new send buffer with a capacity of (size-1)
//
func NewRingBuffer(size uint32) *RingBuffer {
	return &RingBuffer{data: make([]byte, size)}
}

func (s *RingBuffer) WritePos() uint32 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.write
}

func (s *RingBuffer) ReadPos() uint32 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.read
}

// Moves the read position to the specified position.  Must be
// a valid position.
//
func (s *RingBuffer) SetReadPos(pos uint32) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if pos > s.write || pos < s.read {
		return ERR_RINGBUF_READ_INVALID
	}

	s.read = pos
	return nil
}

// Retrieves all the data currently in the buffer.  The returned
// data is copied and changes to it do NOT affect the underlying
// buffer.  Moreover, this has no effect on the write or read positions.
func (s *RingBuffer) Data() []byte {
	s.lock.RLock()
	defer s.lock.RUnlock()

	// grab their positions
	r := s.read
	w := s.write

	len := uint32(len(s.data))
	ret := make([]byte, w-r)

	// just start copying until we get to write
	for i := uint32(0); r+i < w; i++ {
		ret[i] = s.data[(r+i)%len]
	}

	return ret
}

// Reads as many bytes to the given buffer as possible,
// returning the number of bytes that were successfully
// added.  Blocks if NO bytes are available.
//
func (s *RingBuffer) Read(in []byte) (n int, err error) {

	for {
		read := s.tryRead(in)
		if read > 0 {
			// can technically overflow int bounds on 32-bit systems.
			return int(read), nil
		}

		// sleep so as not to overwhelm cpu
		time.Sleep(RINGBUF_WAIT)
	}

	panic("Not accessible")
}

// Adds as many bytes to the given buffer as possible,
// returning the number of bytes that were successfully
// added.  This has the side effect of moving the read
// position.
//
func (s *RingBuffer) tryRead(in []byte) uint32 {
	s.lock.Lock()
	defer s.lock.Unlock()

	// get the new write
	inLen := uint32(len(in))
	bufLen := uint32(len(s.data))

	// grab current positions
	r := s.read
	w := s.write

	var i uint32 = 0
	for ; i < inLen && r+i < w; i++ {
		in[i] = s.data[(r+i)%bufLen]
	}

	s.read = r + i
	return i
}

// Writes the value to the buffer, blocking until it has completed.
//
func (s *RingBuffer) Write(val []byte) (n int, err error) {
	valLen := uint32(len(val))

	for {
		val = val[s.tryWrite(val):]
		if len(val) == 0 {
			break
		}

		// sleep so as not to overwhelm cpu
		time.Sleep(RINGBUF_WAIT)
	}

	return int(valLen), nil
}

// Adds as many bytes to the underlying buffer as possible,
// returning the number of bytes that were successfully
// added.
//
func (s *RingBuffer) tryWrite(val []byte) uint32 {
	s.lock.Lock()
	defer s.lock.Unlock()

	// get the new write
	valLen := uint32(len(val))
	bufLen := uint32(len(s.data))

	// grab current positions
	r := s.read
	w := s.write

	// just write until we can't write anymore.
	var i uint32 = 0
	for ; i < valLen && w+i < r+bufLen; i++ {
		s.data[(w+i)%bufLen] = val[i]
	}

	s.write = s.write + i
	return i
}

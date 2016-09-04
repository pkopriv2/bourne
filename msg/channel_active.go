package msg

import (
	"errors"
	"sync"
	// "sync/atomic"
	"time"
	// "unsafe"
)

// Much of this was inspired by the following papers:
//
// https://tools.ietf.org/html/rfc793
// http://www.ietf.org/proceedings/44/I-D/draft-ietf-sigtran-reliable-udp-00.txt
//

// Defines how many packets will be buffered on receiving
var CHANNEL_RECV_IN_SIZE uint32 = 1024

// Defines how many bytes will be buffered prior to being consumed (should always be greater than send buf)
var CHANNEL_RECV_OUT_SIZE uint32 = 1 << 20 // 1MB

// Defines how many bytes will be buffered by consumer.
var CHANNEL_SEND_IN_SIZE uint32 = 32768

// Defines how many bytes are recalled before an ack is required.
var CHANNEL_SEND_BUF_SIZE uint32 = 1 << 19 //  512K

// Defines how long to wait while channel is transitioning.
var CHANNEL_STATE_WAIT = 50 * time.Millisecond

// The amount of time to wait for an ack update.
var CHANNEL_ACK_TIMEOUT = 5 * time.Second

// Defines how long to wait after each RingBuffer#tryWrite
var RINGBUF_WAIT = 10 * time.Millisecond

// returned when an an attempt to move the read position to an invalid value
var ERR_RINGBUF_READ_INVALID = errors.New("RINGBUF:READ_POSITION_INVALID")

var (
	CHANNEL_EMPTY_BYTE = []byte{}
)

//
// const CHANNEL_RECV_IN_SIZE uint32 = 1024

// CHANNEL_STATES
var (
	CHANNEL_CLOSED               uint64 = 0
	CHANNEL_OPENED               uint64 = 1
	CHANNEL_OPENING_SEQ_SENT     uint64 = 2
	CHANNEL_OPENING_SEQ_RECEIVED uint64 = 3
	CHANNEL_ERR                  uint64 = 255
)

type ChannelState uint64

type ack struct {
	val uint32
	time time.Time
}

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
	recvBuf *BufferedLog

	// send buffers
	sendBuf *BufferedLog
	sendOut chan Packet

	// the state of the channel.  (updated via atomic swaps)
	state *ChannelState

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
	recvOut := NewBufferedLog(CHANNEL_RECV_OUT_SIZE)

	// send queues
	sendBuf := NewBufferedLog(CHANNEL_SEND_BUF_SIZE)
	sendOut := out

	// create the channel
	c := &ChannelActive{
		local:   l,
		remote:  r,
		cache:   cache,
		ids:     ids,
		recvIn:  recvIn,
		recvBuf: recvOut,
		sendBuf: sendBuf,
		sendOut: sendOut}

	// kick off the send
	c.workers.Add(1)
	go func(c *ChannelActive) {
		defer c.workers.Done()

		// the packet buffer (initialized here so we don't continually recreate memory.)
		tmpBuf := make([]byte, PACKET_MAX_DATA_LEN)
		for {
			// // evaluate channel state on every iteration
			// // Anytime we are not in a valid "OPENED" state, give control to the receiver.
			// switch atomic.LoadUint64(&c.state) {
			// case CHANNEL_OPENING_SEQ_RECEIVED :
			// time.Sleep(CHANNEL_STATE_WAIT)
			// continue
			// case CHANNEL_OPENING_SEQ_SENT :
			// time.Sleep(CHANNEL_STATE_WAIT)
			// continue
			// case CHANNEL_OPENED :
			// // normal state...continue
			// break
			// }

			// eventually, we're going to want to *size* our packets according to
			// what the receiver can receive at the time.  We'll do this by analyzing
			// the difference between the send rate and the ack rate.  For now,
			// we'll just

			// start building the outgoing packet.
			flags := PACKET_FLAG_ACK

			// see if we should be sending a data packet.
			num, seq := c.sendBuf.TryReadPending(tmpBuf)

			// build the packet data.
			data := tmpBuf[:num]
			if num > 0 {
				flags |= PACKET_FLAG_SEQ
			}

			// if no data, we're only going to be sending an ack, so wait a little bit.
			// so as not to constantly be sending empty packets
			if num == 0 {
				time.Sleep(CHANNEL_STATE_WAIT)
			}


			// okay, send the packet
			c.sendOut <- *c.newSendPacket(flags, seq, c.recvBuf.WritePos(), data)
		}
	}(c)

	// kick off the recv
	c.workers.Add(1)
	go func(c *ChannelActive) {
		defer c.workers.Done()
		for {
		}
	}(c)

	// add it to the channel pool (i.e. make it available for routing)
	if err := cache.Add(l, c); err != nil {
		return nil, err
	}

	// finally, return it.
	return c, nil
}

// Generates a new
func (self *ChannelActive) newSendPacket(ctrls uint8, seq uint32, ack uint32, data []byte) *Packet {
	return &Packet{PROTOCOL_VERSION, self.local.entityId, self.local.channelId, self.remote.entityId, self.remote.channelId, ctrls, seq, ack, data}
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

	return self.recvBuf.Read(buf)
}

// Writes data to the channel.  Blocks if the underlying buffer is full.
//
func (self *ChannelActive) Write(data []byte) (int, error) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.closed {
		return 0, CHANNEL_CLOSED_ERROR
	}

	return self.sendBuf.Write(data)
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
// TODO: Handle integer overflow
//
type BufferedLog struct {
	data []byte
	lock sync.RWMutex // just using simple, coarse lock

	write       uint32
	readCommit  uint32
	readPending uint32
}

// Returns a new send buffer with a capacity of (size-1)
//
func NewBufferedLog(size uint32) *BufferedLog {
	return &BufferedLog{data: make([]byte, size)}
}

func (s *BufferedLog) WritePos() uint32 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.write
}

func (s *BufferedLog) ReadPos() uint32 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.readCommit
}

// Resets the pending reads to the last committed read.
//
func (s *BufferedLog) RollBack() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.readPending = s.readCommit
	return nil
}

// Commits the log
//
func (s *BufferedLog) Commit(pos uint32) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if pos > s.write || pos < s.readCommit {
		return ERR_RINGBUF_READ_INVALID
	}

	s.readCommit = pos
	return nil
}

// Retrieves all the uncommited data currently in the buffer.  The returned
// data is copied and changes to it do NOT affect the underlying
// buffer.  Moreover, this has no effect on the write or read positions.
func (s *BufferedLog) Data() []byte {
	s.lock.RLock()
	defer s.lock.RUnlock()

	// grab their positions
	r := s.readCommit
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
func (s *BufferedLog) Read(in []byte) (n int, err error) {

	for {
		read := s.TryReadCommit(in)
		if read > 0 {
			return int(read), nil
		}

		// sleep so as not to overwhelm cpu
		time.Sleep(RINGBUF_WAIT)
	}

	panic("Not accessible")
}

// Reads from the buffer, but DOES NOT move the read pointer.
//
func (s *BufferedLog) TryReadPending(in []byte) (uint32, uint32) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// get the new write
	inLen := uint32(len(in))
	bufLen := uint32(len(s.data))

	// grab current positions
	r := s.readPending
	w := s.write

	var i uint32 = 0
	for ; i < inLen && r+i < w; i++ {
		in[i] = s.data[(r+i)%bufLen]
	}

	s.readPending = r + i
	return i, s.readPending
}

// Reads as many bytes from the underlying buffer as possible,
//
func (s *BufferedLog) TryReadCommit(in []byte) uint32 {
	s.lock.Lock()
	defer s.lock.Unlock()

	// get the new write
	inLen := uint32(len(in))
	bufLen := uint32(len(s.data))

	// grab current positions
	r := s.readCommit
	w := s.write

	var i uint32 = 0
	for ; i < inLen && r+i < w; i++ {
		in[i] = s.data[(r+i)%bufLen]
	}

	s.readPending = r + i
	s.readCommit = s.readPending
	return i
}

// Writes the value to the buffer, blocking until it has completed.
//
func (s *BufferedLog) Write(val []byte) (n int, err error) {
	valLen := uint32(len(val))

	for {
		val = val[s.TryWrite(val):]
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
// added.  Unlike io.Writer#Write(...) this method may
// return fewer bytes than intended.
//
func (s *BufferedLog) TryWrite(val []byte) uint32 {
	s.lock.Lock()
	defer s.lock.Unlock()

	// get the new write
	valLen := uint32(len(val))
	bufLen := uint32(len(s.data))

	// grab current positions
	r := s.readCommit
	w := s.write

	// just write until we can't write anymore.
	var i uint32 = 0
	for ; i < valLen && w+i < r+bufLen; i++ {
		s.data[(w+i)%bufLen] = val[i]
	}

	s.write = s.write + i
	return i
}

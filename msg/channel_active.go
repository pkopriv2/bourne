package msg

import (
	"errors"
	"fmt"
	"sync"
	// "sync/atomic"
	// "log"
	"time"
	// "unsafe"

	"github.com/rcrowley/go-metrics"
)

// Much of this was inspired by the following papers:
//
// https://tools.ietf.org/html/rfc793
// http://www.ietf.org/proceedings/44/I-D/draft-ietf-sigtran-reliable-udp-00.txt
//
var (
	ERR_RINGBUF_ACK_INVALID = errors.New("RINGBUF:READ_POSITION_INVALID")
)

const (
	CHANNEL_DEFAULT_RECV_IN_SIZE  = 1024
	CHANNEL_DEFAULT_RECV_BUF_SIZE = 1 << 20 // 1MB
	CHANNEL_DEFAULT_SEND_BUF_SIZE = 1 << 10  // 256K
	CHANNEL_DEFAULT_SEND_WAIT     = 100 * time.Millisecond
	CHANNEL_DEFAULT_RECV_WAIT     = 20 * time.Millisecond
	CHANNEL_DEFAULT_ACK_TIMEOUT   = 1 * time.Second
	DATALOG_LOCK_WAIT             = 10 * time.Millisecond
)

type ChannelOptions struct {

	// Defines how many packets will be buffered before blocking
	RecvInSize uint

	// Defines how many bytes will be buffered prior to being consumed (should always be greater than send buf)
	RecvBufSize uint

	// Defines how many bytes will be buffered prior to being consumed (should always be greater than send buf)
	SendBufSize uint

	// The duration to wait before trying to send data again (when none was available)
	SendWait time.Duration

	// The duration to wait before trying to fetch again (when none was avaiable)
	RecvWait time.Duration

	// The duration to wait for an ack before data is considered lost
	AckTimeout time.Duration

	// The duration to wait on incremental writes to the log
	LogWait time.Duration

	// // to be called when the channel is closed (allows the release of external resources (ie ids, routing table))
	// OnData func(*Packet) error

	// to be called when the channel has been initialized, and is in the process of being started
	OnInit func(*ChannelActive) error

	// to be called when the channel is closed (allows the release of external resources (ie ids, routing table))
	OnClose func(*ChannelActive) error

	// to be called when the channel is
	OnError func(*ChannelActive, error) error
}

func DefaultChannelOptions() *ChannelOptions {
	return &ChannelOptions{
		RecvInSize:  CHANNEL_DEFAULT_RECV_IN_SIZE,
		RecvBufSize: CHANNEL_DEFAULT_RECV_BUF_SIZE,
		SendBufSize: CHANNEL_DEFAULT_SEND_BUF_SIZE,
		SendWait:    CHANNEL_DEFAULT_SEND_WAIT,
		RecvWait:    CHANNEL_DEFAULT_RECV_WAIT,
		AckTimeout:  CHANNEL_DEFAULT_ACK_TIMEOUT,
		LogWait:     DATALOG_LOCK_WAIT,

		OnInit:  func(c *ChannelActive) error { return nil },
		OnClose: func(c *ChannelActive) error { return nil },
		OnError: func(c *ChannelActive, error error) error { return nil }}
}

type ChannelConfig func(*ChannelOptions)

// CHANNEL_STATES
// var (
// CHANNEL_CLOSED               uint64 = 0
// CHANNEL_OPENED               uint64 = 1
// CHANNEL_OPENING_SEQ_SENT     uint64 = 2
// CHANNEL_OPENING_SEQ_RECEIVED uint64 = 3
// CHANNEL_ERR                  uint64 = 255
// )
//
// type ChannelState uint64

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

	// receive buffers
	recvIn  chan Packet
	recvBuf *DataLog

	// send buffers
	sendBuf *DataLog
	sendOut chan Packet

	// the state of the channel.  (updated via atomic swaps)
	// state *ChannelState
	options *ChannelOptions

	// the workers wait
	workers sync.WaitGroup

	// a flag indicating that the channel is closed. (synchronized on lock)
	closed bool

	// general channel statistics
	stats *ChannelStats

	// the channel's lock
	lock sync.RWMutex
}

// Creates and returns a new channel.
//
func NewChannelActive(l ChannelAddress, r ChannelAddress, out chan Packet, opts ...ChannelConfig) (*ChannelActive, error) {
	// initialize the options.
	options := DefaultChannelOptions()
	for _, opt := range opts {
		opt(options)
	}

	// create the channel
	c := &ChannelActive{
		local:   l,
		remote:  r,
		stats:   NewChannelStats(l),
		options: options,
		recvIn:  make(chan Packet, options.RecvInSize),
		recvBuf: NewDataLog(options.RecvBufSize),
		sendBuf: NewDataLog(options.SendBufSize),
		sendOut: out}

	// kick off the send worker
	c.workers.Add(1)
	go func(c *ChannelActive) {
		defer c.workers.Done()

		// the packet buffer (initialized here so we don't continually recreate memory.)
		tmpBuf := make([]byte, PACKET_MAX_DATA_LEN)

		// track the last ack value
		lastAckOffset := uint32(0)
		for {
			// evaluate channel state on every iteration
			// Anytime we are not in a valid "OPENED" state, give control to the receiver.
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

			// start building the outgoing packet.
			flags := PACKET_FLAG_NONE

			// see if we should be sending a data packet.
			num, start := c.sendBuf.TryRead(tmpBuf, false)

			// build the packet data.
			data := tmpBuf[:num]
			if num > 0 {
				flags = flags | PACKET_FLAG_DAT
			}

			// the sent ack is based on what's been received.
			recv := c.recvBuf.WritePos()
			if recv.offset > lastAckOffset {
				flags = flags | PACKET_FLAG_ACK
				lastAckOffset = recv.offset
			}

			if flags == 0 {
				time.Sleep(c.options.SendWait)
				continue
			}

			c.stats.packetsSent.Inc(1)
			c.sendOut <- *NewPacket(c.local.entityId, c.local.channelId, c.remote.entityId, c.remote.channelId, flags, start.offset, recv.offset, 100, data)
		}
	}(c)

	// kick off the recv
	c.workers.Add(1)
	go func(c *ChannelActive) {
		defer c.workers.Done()

		for {

			select {
			case p, ok := <-c.recvIn:
				if !ok {
					return
				}

				c.stats.packetsReceived.Inc(1)

				// Handle: ack flag (move the log start position)
				if p.ctrls&PACKET_FLAG_ACK > 0 {
					if err := c.sendBuf.Prune(p.ack); err != nil {
						c.stats.packetsDropped.Inc(1)
						break
					}
				}

				// Handle: data flag (consume elements of the stream)
				// TODO: REALLY NEED TO SUPPORT OUT OF ORDER (IE DROPPED PACKETS)
				if p.ctrls&PACKET_FLAG_DAT > 0 {
					write := c.recvBuf.WritePos()
					if p.offset > write.offset {
						c.stats.bytesDropped.Inc(int64(len(p.data)))
						c.stats.packetsDropped.Inc(1)
						break
					}

					if write.offset > p.offset+uint32(len(p.data)) {
						c.stats.bytesDropped.Inc(int64(len(p.data)))
						c.stats.packetsDropped.Inc(1)
						break
					}

					c.stats.bytesReceived.Inc(int64(len(p.data)))
					c.recvBuf.Write(p.data[write.offset-p.offset:])
				}

			default:
				time.Sleep(c.options.RecvWait)
				break
			}

			// if we haven't received a valid ack in a while, we need to begin retransmitting
			start := c.sendBuf.StartPos()
			if time.Since(start.time) >= c.options.AckTimeout {
				before, after := c.sendBuf.ResetRead()
				c.stats.numResets.Inc(1)
				c.stats.bytesReset.Inc(int64(before.offset - after.offset))
			}
		}
	}(c)

	// finally, return it.
	return c, nil
}

// Generates a new
func (self *ChannelActive) newSendPacket(ctrls uint8, offset uint32, ack uint32, win uint32, data []byte) *Packet {
	return NewPacket(self.local.entityId, self.local.channelId, self.remote.entityId, self.remote.channelId, ctrls, offset, ack, win, data)
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

	// close all 'owned' io objects
	// close(self.recvIn)
	// close(self.recvOut)
	// close(self.sendIn)

	// finally, wait for the workers to be done.
	self.workers.Wait()
	self.closed = true
	return nil
}

type ChannelStats struct {
	packetsSent     metrics.Counter
	packetsDropped  metrics.Counter
	packetsReceived metrics.Counter

	bytesSent     metrics.Counter
	bytesDropped  metrics.Counter
	bytesReceived metrics.Counter
	bytesReset    metrics.Counter

	numResets metrics.Counter
}

func NewChannelMetricName(addr ChannelAddress, name string) string {
	return fmt.Sprintf("-- %+v --: %s", addr, name)
}

func NewChannelStats(addr ChannelAddress) *ChannelStats {
	r := metrics.DefaultRegistry

	return &ChannelStats{
		packetsSent: metrics.NewRegisteredCounter(
			NewChannelMetricName(addr, "channel.PacketsSent"), r),
		packetsReceived: metrics.NewRegisteredCounter(
			NewChannelMetricName(addr, "channel.PacketsReceived"), r),
		packetsDropped: metrics.NewRegisteredCounter(
			NewChannelMetricName(addr, "channel.PacketsDropped"), r),

		bytesSent: metrics.NewRegisteredCounter(
			NewChannelMetricName(addr, "channel.BytesSent"), r),
		bytesReceived: metrics.NewRegisteredCounter(
			NewChannelMetricName(addr, "channel.BytesReceived"), r),
		bytesDropped: metrics.NewRegisteredCounter(
			NewChannelMetricName(addr, "channel.BytesDropped"), r),
		bytesReset: metrics.NewRegisteredCounter(
			NewChannelMetricName(addr, "channel.BytesReset"), r),
		numResets: metrics.NewRegisteredCounter(
			NewChannelMetricName(addr, "channel.NumResets"), r)}
}

type Position struct {
	offset uint32
	time   time.Time
}

func NewPosition(offset uint32) Position {
	return Position{offset, time.Now()}
}

// A simple bounded data log that tracks
//
type DataLog struct {
	data []byte
	lock sync.RWMutex // just using simple, coarse lock

	start Position
	read  Position
	write Position
}

// Returns a new send buffer with a capacity of (size-1)
//
func NewDataLog(size uint) *DataLog {
	return &DataLog{data: make([]byte, size)}
}

func (s *DataLog) WritePos() Position {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.write
}

func (s *DataLog) ReadPos() Position {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.read
}

func (s *DataLog) StartPos() Position {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.start
}

func (s *DataLog) ResetRead() (Position, Position) {
	s.lock.Lock()
	defer s.lock.Unlock()

	before := s.read

	s.start = NewPosition(before.offset)
	s.read = s.start
	return before, s.read
}

func (s *DataLog) Prune(pos uint32) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if pos > s.write.offset {
		return ERR_RINGBUF_ACK_INVALID
	}

	if pos < s.start.offset {
		return nil
	}

	s.start = NewPosition(pos)
	return nil
}

func (s *DataLog) Data() []byte {
	s.lock.RLock()
	defer s.lock.RUnlock()

	// grab their positions
	r := s.read.offset
	w := s.write.offset

	len := uint32(len(s.data))
	ret := make([]byte, w-r)

	// just start copying until we get to write
	for i := uint32(0); r+i < w; i++ {
		ret[i] = s.data[(r+i)%len]
	}

	return ret
}

// Reads from the buffer from the current positon.
//
func (s *DataLog) TryRead(in []byte, prune bool) (uint32, Position) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// get the new write
	inLen := uint32(len(in))
	bufLen := uint32(len(s.data))

	// grab the current read offset.
	start := s.read

	// grab current positions
	r := start.offset
	w := s.write.offset

	var i uint32 = 0
	for ; i < inLen && r+i < w; i++ {
		in[i] = s.data[(r+i)%bufLen]
	}

	s.read = NewPosition(r + i)

	// are we moving the start position?
	if prune {
		s.start = s.read
	}

	return i, start
}

func (s *DataLog) TryWrite(val []byte) (uint32, Position) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// get the new write
	valLen := uint32(len(val))
	bufLen := uint32(len(s.data))

	// grab current positions
	r := s.start.offset
	w := s.write.offset

	// just write until we can't write anymore.
	var i uint32 = 0
	for ; i < valLen && w+i < r+bufLen; i++ {
		s.data[(w+i)%bufLen] = val[i]
	}

	// s.start = NewPosition(s.start.offset)
	s.write = NewPosition(w + i)
	return i, s.write
}

func (s *DataLog) Write(val []byte) (int, error) {
	valLen := uint32(len(val))

	for {
		num, _ := s.TryWrite(val)

		val = val[num:]
		if len(val) == 0 {
			break
		}

		time.Sleep(DATALOG_LOCK_WAIT)
	}

	return int(valLen), nil
}

func (s *DataLog) Read(in []byte) (n int, err error) {
	for {
		read, _ := s.TryRead(in, true)
		if read > 0 {
			return int(read), nil
		}

		time.Sleep(DATALOG_LOCK_WAIT)
	}

	panic("Not accessible")
}

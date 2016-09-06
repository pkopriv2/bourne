package msg

import (
	"errors"
	"fmt"
	// "fmt"
	// "log"
	"sync"
	"time"
)

// Much of this was inspired by the following papers:
//
// https://tools.ietf.org/html/rfc793
// http://www.ietf.org/proceedings/44/I-D/draft-ietf-sigtran-reliable-udp-00.txt
//
var (
	ERR_LOG_PRUNE_INVALID = errors.New("LOG:INVALID_PRUNE")
)

const (
	CHANNEL_DEFAULT_RECV_IN_SIZE  = 1024
	CHANNEL_DEFAULT_RECV_LOG_SIZE = 1 << 20 // 1024K
	CHANNEL_DEFAULT_SEND_LOG_SIZE = 1 << 18 // 256K
	CHANNEL_DEFAULT_SEND_WAIT     = 100 * time.Millisecond
	CHANNEL_DEFAULT_RECV_WAIT     = 20 * time.Millisecond
	CHANNEL_DEFAULT_ACK_TIMEOUT   = 5 * time.Second
	DATALOG_LOCK_WAIT             = 5 * time.Millisecond
)

type ChannelOptions struct {
	// Whether or not to enable debug loggin.
	SendDebug bool

	// Whether or not to enable debug loggin.
	RecvDebug bool

	// Defines how many packets will be buffered before blocking
	RecvInSize uint

	// Defines how many bytes will be buffered prior to being consumed (should always be greater than send buf)
	RecvLogSize uint

	// Defines how many bytes will be buffered prior to being consumed (should always be greater than send buf)
	SendLogSize uint

	// The duration to wait before trying to send data again (when none was available)
	SendWait time.Duration

	// The duration to wait before trying to fetch again (when none was avaiable)
	RecvWait time.Duration

	// The duration to wait for an ack before data is considered lost
	AckTimeout time.Duration

	// The duration to wait on incremental writes to the log
	LogWait time.Duration

	// to be called when the channel has been initialized, and is in the process of being started
	OnInit func(*ChannelActive) error

	// to be called when the channel is closed (allows the release of external resources (ie ids, routing table))
	OnClose func(*ChannelActive) error

	// to be called when the channel is
	OnError func(*ChannelActive, error) error

	// to be called when the channel produces an outgoing packet. (may block)
	OnData func(*Packet) error
}

func DefaultChannelOptions() *ChannelOptions {
	return &ChannelOptions{
		SendDebug: false,

		RecvInSize:  CHANNEL_DEFAULT_RECV_IN_SIZE,
		RecvLogSize: CHANNEL_DEFAULT_RECV_LOG_SIZE,
		SendLogSize: CHANNEL_DEFAULT_SEND_LOG_SIZE,
		SendWait:    CHANNEL_DEFAULT_SEND_WAIT,
		RecvWait:    CHANNEL_DEFAULT_RECV_WAIT,
		AckTimeout:  CHANNEL_DEFAULT_ACK_TIMEOUT,
		LogWait:     DATALOG_LOCK_WAIT,

		OnInit:  func(c *ChannelActive) error { return nil },
		OnClose: func(c *ChannelActive) error { return nil },
		OnError: func(c *ChannelActive, error error) error { return nil },
		OnData:  func(p *Packet) error { return nil }}
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

	// channel options. (SHOULD NOT BE MUTATED)
	options *ChannelOptions

	// general channel statistics
	stats *ChannelStats

	// receive buffers
	recvIn  chan Packet
	recvLog *Stream

	// send buffers
	sendLog *Stream
	sendOut chan Packet

	// the state of the channel.  (updated via atomic swaps)
	// state *ChannelState

	// the workers wait
	workers sync.WaitGroup

	// a flag indicating that the channel is closed. (synchronized on lock)
	closed bool

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
		recvLog: NewStream(options.RecvLogSize),
		sendLog: NewStream(options.SendLogSize),
		sendOut: out}

	// kick off the workers
	c.workers.Add(1)
	go func(c *ChannelActive) {
		defer c.workers.Done()
		sendWorker(c)
	}(c)

	c.workers.Add(1)
	go func(c *ChannelActive) {
		defer c.workers.Done()
		recvWorker(c)
	}(c)

	// finally, return it.
	return c, nil
}

// type Logger struct {
// enabled bool
// Channel owner
// }
//
//
// func (self *Logger) Log(format string, vals ...interface{}) {
// if !self.enabled {
// return
// }
//
// log.Println(fmt.Sprintf("[%v] -- ", self.local) + fmt.Sprintf(format, vals...))
// }

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

	return self.recvLog.Read(buf)
}

// Writes data to the channel.  Blocks if the underlying buffer is full.
//
func (self *ChannelActive) Write(data []byte) (int, error) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.closed {
		return 0, CHANNEL_CLOSED_ERROR
	}

	return self.sendLog.Write(data)
}

// Sends a packet to the receive packet stream.
//
func (self *ChannelActive) Send(p *Packet) error {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.closed {
		return CHANNEL_CLOSED_ERROR
	}

	fmt.Println("Sending packet")
	self.recvIn <- *p
	return nil
}

// Closes the channel.  Returns an error if the channel is already closed.
//
func (self *ChannelActive) Close() error {
	self.lock.Lock()
	defer self.lock.Unlock()
	if self.closed {
		return CHANNEL_CLOSED_ERROR
	}

	self.options.OnClose(self)

	// close all 'owned' io objects
	close(self.recvIn)

	// finally, wait for the workers to be done.
	self.workers.Wait()
	self.closed = true
	return nil
}

func sendWorker(c *ChannelActive) {
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

		// if we haven't received an ack in a while, start retransmitting
		sendTail, _, _ := c.sendLog.Refs()
		if time.Since(sendTail.time) >= c.options.AckTimeout {
			before, after := c.sendLog.Reset()
			fmt.Printf("RESET SEND LOG [%v, %v]\n", before.offset, after.offset)
		}

		// start building the outgoing packet.
		flags := PACKET_FLAG_NONE

		// see if we should be sending a data packet.
		sendStart, num := c.sendLog.TryRead(tmpBuf, false)

		// build the packet data.
		data := tmpBuf[:num]
		if num > 0 {
			flags = flags | PACKET_FLAG_DAT
		}

		// the sent ack is based on what's been received.
		_, _, recvHead := c.recvLog.Refs()
		if recvHead.offset > lastAckOffset {
			flags = flags | PACKET_FLAG_ACK
			lastAckOffset = recvHead.offset
		}

		if flags == 0 {
			time.Sleep(c.options.SendWait)
			continue
		}

		c.stats.packetsSent.Inc(1)
		c.sendOut <- *NewPacket(c.local.entityId, c.local.channelId, c.remote.entityId, c.remote.channelId, flags, sendStart.offset, recvHead.offset, 0, data)
	}
}

func recvWorker(c *ChannelActive) {
	defer panic("OH NO")

	nextPacket := func() (*Packet, error) {
		select {
		case p, ok := <-c.recvIn:
			if !ok {
				return nil, ERR_CHANNEL_CLOSED
			}

			return &p, nil
		default:
			return nil, nil
		}
	}

	for {
		p, err := nextPacket()
		if err != nil {
			return
		}

		if p == nil {
			time.Sleep(c.options.RecvWait)
			continue
		}

		c.stats.packetsReceived.Inc(1)

		// Handle: ack flag (move the log tail position)
		if p.ctrls&PACKET_FLAG_ACK > 0 {
			if err := c.sendLog.Commit(p.ack); err != nil {
				c.stats.packetsDropped.Inc(1)
				continue
			}
		}

		// Handle: data flag (consume elements of the stream)
		// TODO: REALLY NEED TO SUPPORT OUT OF ORDER (IE DROPPED PACKETS)
		if p.ctrls&PACKET_FLAG_DAT > 0 {
			_, _, head := c.recvLog.Refs()
			if head.offset > p.offset+uint32(len(p.data)) {
				c.stats.bytesDropped.Inc(int64(len(p.data)))
				c.stats.packetsDropped.Inc(1)
				continue
			}

			if p.offset > head.offset {
				// fmt.Println("DROPPED PACKET.  FUTURE OFFSET")
				c.stats.bytesDropped.Inc(int64(len(p.data)))
				c.stats.packetsDropped.Inc(1)
				continue
			}

			c.stats.bytesReceived.Inc(int64(len(p.data)))
			c.recvLog.Write(p.data[head.offset-p.offset:])
		}
	}
}

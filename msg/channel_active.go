package msg

import (
	"errors"
	"fmt"
	"log"
	// "fmt"
	// "log"
	"sync"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
)

// Much of this was inspired by the following papers:
//
// https://tools.ietf.org/html/rfc793
// http://www.ietf.org/proceedings/44/I-D/draft-ietf-sigtran-reliable-udp-00.txt
//
var (
	ErrChannelClosed          = errors.New("CHAN:CLOSED")
	ErrChannelUnexpectedState = errors.New("CHAN:UNEXPECTED_STATE")
)

// default channel options
const (
	ChannelStateWait              = 1 * time.Millisecond
	CHANNEL_DEFAULT_RECV_IN_SIZE  = 1024
	CHANNEL_DEFAULT_RECV_LOG_SIZE = 1 << 20 // 1024K
	CHANNEL_DEFAULT_SEND_LOG_SIZE = 1 << 18 // 256K
	CHANNEL_DEFAULT_SEND_WAIT     = 100 * time.Millisecond
	CHANNEL_DEFAULT_RECV_WAIT     = 20 * time.Millisecond
	CHANNEL_DEFAULT_ACK_TIMEOUT   = 1 * time.Second
	CHANNEL_DEFAULT_WIN_TIMEOUT   = 2 * time.Second
	DATALOG_LOCK_WAIT             = 5 * time.Millisecond
)

// function used to configure a channel. (accepted as part of construction)
type ChannelConfig func(*ChannelOptions)

// function called on channel state changes.
type ChannelStateFn func(*ChannelActive) error

// returns the default channel options struct
func DefaultChannelOptions() *ChannelOptions {
	return &ChannelOptions{
		RecvInSize:  CHANNEL_DEFAULT_RECV_IN_SIZE,
		RecvLogSize: CHANNEL_DEFAULT_RECV_LOG_SIZE,
		SendLogSize: CHANNEL_DEFAULT_SEND_LOG_SIZE,
		SendWait:    CHANNEL_DEFAULT_SEND_WAIT,
		RecvWait:    CHANNEL_DEFAULT_RECV_WAIT,
		AckTimeout:  CHANNEL_DEFAULT_ACK_TIMEOUT,
		WinTimeout:  CHANNEL_DEFAULT_WIN_TIMEOUT,
		LogWait:     DATALOG_LOCK_WAIT,

		// handlers
		OnInit:  func(c *ChannelActive) error { return nil },
		OnOpen:  func(c *ChannelActive) error { return nil },
		OnClose: func(c *ChannelActive) error { return nil },
		OnError: func(c *ChannelActive) error { return nil },
		OnData:  func(p *Packet) error { return nil }}
}

// options struct
type ChannelOptions struct {

	// Whether or not to enable debug logging.
	Debug bool

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

	// The duration to wait for an ack before data is considered lost
	WinTimeout time.Duration

	// The duration to wait on incremental writes to the log
	LogWait time.Duration

	// to be called when the channel has been initialized, and is in the process of being started
	OnInit ChannelStateFn

	// to be called when the channel has encountered an unrecoverable error
	OnOpen ChannelStateFn

	// to be called when the channel is closed (allows the release of external resources (ie ids, routing table))
	OnClose ChannelStateFn

	// to be called when the channel has encountered an unrecoverable error
	OnError ChannelStateFn

	// to be called when the channel produces an outgoing packet. (may block)
	OnData func(*Packet) error
}

type ChannelState uint32

// channel states
const (
	ChannelInit ChannelState = iota
	ChannelOpened
	ChannelClosed
)

type ChannelStateMachine struct {
	sync.RWMutex
	cur ChannelState
}

func NewChannelStateMachine(init ChannelState) *ChannelStateMachine {
	return &ChannelStateMachine{cur: init}
}

func (c *ChannelStateMachine) Get() ChannelState {
	c.RLock()
	defer c.RUnlock()
	return c.cur
}

func (c *ChannelStateMachine) Synchronize(fn func(ChannelState)) {
	c.RLock()
	defer c.RUnlock()
	fn(c.cur)
}

func (c *ChannelStateMachine) SynchronizeIf(expected ChannelState, fn func()) error {
	c.RLock()
	defer c.RUnlock()

	if c.cur != expected {
		return ErrChannelUnexpectedState
	}

	fn()
	return nil
}

func (c *ChannelStateMachine) Transition(from ChannelState, fn func() ChannelState) error {
	c.Lock()
	defer c.Unlock()

	if c.cur != from {
		return ErrChannelUnexpectedState
	}

	c.cur = fn()
	return nil
}

func (c *ChannelStateMachine) WaitUntil(terminalStates ...ChannelState) ChannelState {
	// just spin, waiting for an appropriate state
	for {
		s := c.Get()
		for _, t := range terminalStates {
			if s == t {
				return s
			}
		}

		time.Sleep(ChannelStateWait)
	}
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
//   * Senders currently detect drops based on timeouts.
//     (currently working to use duplicate ack detection)
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

	// the state of the channel. (used to enforce channel invariants.)
	state *ChannelStateMachine

	// receive buffers
	recvIn  chan Packet
	recvLog *Stream

	// send buffers
	sendLog *Stream
	sendOut func(p *Packet) error

	// event handlers
	OnInit  ChannelStateFn
	OnOpen  ChannelStateFn
	OnClose ChannelStateFn
	OnError ChannelStateFn

	// the workers wait
	workers sync.WaitGroup
}

// Creates and returns a new channel.
//
func NewChannelActive(l ChannelAddress, r ChannelAddress, opts ...ChannelConfig) (*ChannelActive, error) {
	// initialize the options.
	options := DefaultChannelOptions()
	for _, opt := range opts {
		opt(options)
	}

	state := NewChannelStateMachine(ChannelInit)

	// create the channel
	c := &ChannelActive{
		local:   l,
		remote:  r,
		stats:   NewChannelStats(l),
		state:   state,
		options: options,
		recvIn:  make(chan Packet, options.RecvInSize),
		recvLog: NewStream(options.RecvLogSize),
		sendLog: NewStream(options.SendLogSize),
		sendOut: options.OnData,
		OnInit:  options.OnInit,
		OnError: options.OnError}

	// call the state fn. (no synchronization required)
	c.OnInit(c)

	// kick off the workers
	c.workers.Add(2)
	go sendWorker(c)
	go recvWorker(c)

	// finally, return it.
	return c, nil
}

// Logs a message, tagging it with the channel's local address.
func (c *ChannelActive) Log(format string, vals ...interface{}) {
	if !c.options.Debug {
		return
	}

	log.Println(fmt.Sprintf("[%v] -- ", c.local) + fmt.Sprintf(format, vals...))
}

// Returns the local address of this channel
func (c *ChannelActive) LocalAddr() ChannelAddress {
	return c.local
}

// Returns the remote address of this channel
func (c *ChannelActive) RemoteAddr() ChannelAddress {
	return c.remote
}

// Reads data from the channel.  Blocks if data isn't available.
func (c *ChannelActive) Read(buf []byte) (int, error) {
	c.state.WaitUntil(ChannelOpened, ChannelClosed)

	var num int
	var err error

	num, err = 0, ErrChannelClosed
	c.state.SynchronizeIf(ChannelOpened, func() {
		num, err = c.recvLog.Read(buf)
	})

	return num, err
}

// Writes the data to the channel.  Blocks if the underlying send buffer is full.
func (c *ChannelActive) Write(data []byte) (int, error) {
	c.state.WaitUntil(ChannelOpened, ChannelClosed)

	var num int
	var err error

	num, err = 0, ErrChannelClosed
	c.state.SynchronizeIf(ChannelOpened, func() {
		num, err = c.sendLog.Write(data)
	})

	return num, err
}

// Sends a packet to the receive packet stream.
func (c *ChannelActive) Send(p *Packet) error {
	c.state.WaitUntil(ChannelOpened, ChannelClosed)

	err := c.state.SynchronizeIf(ChannelOpened, func() {
		c.recvIn <- *p
	})

	if err != nil {
		return ErrChannelClosed
	} else {
		return nil
	}

}

// Closes the channel.  Returns an error if the channel is already closed.
func (c *ChannelActive) Close() error {
	err := c.state.Transition(ChannelOpened, func() ChannelState {
		c.options.OnClose(c)
		return ChannelClosed
	})

	// if error, we didn't transition.  stop now.
	if err != nil {
		return err
	}

	// close all 'owned' io objects
	close(c.recvIn)
	c.workers.Wait()
	return nil
}

func sendWorker(c *ChannelActive) {
	defer c.workers.Done()

	// give exclusive access to receiver while waiting to open.

	// initialize the timeout values
	// timeout := c.options.AckTimeout
	// timeoutCnt := 0
	//
	// // track last ack received
	// recvack, _, _ := c.sendlog.refs()

	// track last ack sent
	_, _, sendAck := c.recvLog.Refs()

	// the packet buffer (initialized here so we don't continually recreate memory.)
	tmp := make([]byte, PacketMaxLength)
	for {
		state := c.state.WaitUntil(ChannelOpened, ChannelClosed)
		if state != ChannelOpened {
			return
		}

		// let's see if we need to retransmit
		// sendTail, _, _ := c.sendLog.Refs()

		// if we received an ack recently, reset the timeout counters
		// if sendTail.offset > recvAck.offset {
		// recvAck = sendTail
		// timeoutCur = timeoutInit
		// timeoutCnt = 0
		// }

		// // let's see if we're in a timeout senario.
		// if time.Since(sendTail.time) >= timeoutCur {
		// cur, prev := c.sendLog.Reset()
		// c.Log("Ack timed out. Reset send log to [%v] from [%v]", cur.offset, prev.offset)
		//
		// timeoutCur *= 2
		// c.Log("Increasing ack timeout to [%v]", timeoutCur)
		//
		// if timeoutCnt += 1; timeoutCnt > 3 {
		// c.Log("Max timeouts reached")
		// return
		// }
		// }
		//
		// // stop sending if we're timing out.
		// if timeoutCnt > 0 {
		// c.Log("Currently waiting for acks before sending data")
		// time.Sleep(c.options.SendWait)
		// continue
		// }

		// start building the outgoing packet
		flags := PacketFlagNone

		// see if we should be sending a data packet.
		sendStart, num := c.sendLog.TryRead(tmp, false)

		// build the packet data.
		data := tmp[:num]
		if num > 0 {
			flags = flags | PacketFlagData
		}

		// see if we should be sending an ack.
		_, _, recvHead := c.recvLog.Refs()
		if recvHead.offset > sendAck.offset {
			flags = flags | PacketFlagAck
			sendAck = recvHead
		}

		// just sleep if nothing to do
		if flags == PacketFlagNone {
			time.Sleep(c.options.SendWait)
			continue
		}

		// finally, send the packet.
		err := c.state.SynchronizeIf(ChannelOpened, func() {
			c.sendOut(
				NewPacket(
					c.local.entityId,
					c.local.channelId,
					c.remote.entityId,
					c.remote.channelId,
					flags,
					sendStart.offset,
					recvHead.offset,
					0,
					data))
		})

		if err != nil {
			c.Log("Cannot send data.  Channel has been closed.")
			return
		}

		c.stats.packetsSent.Inc(1)
	}
}

func recvWorker(c *ChannelActive) {
	defer c.workers.Done()

	nextPacket := func() (*Packet, error) {
		select {
		case p, ok := <-c.recvIn:
			if !ok {
				return nil, ErrChannelClosed
			}

			return &p, nil
		default:
			return nil, nil
		}
	}

	// we'll use a simple sorted tree map to track out of order segments
	pending := treemap.NewWith(OffsetComparator)
	for {
		switch c.state.Get() {
		case ChannelInit:
			c.state.Transition(ChannelInit, func() ChannelState {
				return ChannelOpened
			})
			continue
		case ChannelOpened:
			break
		case ChannelClosed:
			return
		}

		p, err := nextPacket()
		if err != nil {
			return
		}

		if p == nil {
			time.Sleep(c.options.RecvWait)
			continue
		}

		c.stats.packetsReceived.Inc(1)
		// c.Log("Packet received: %v", p)

		// Handle: ack flag
		if p.ctrls&PacketFlagAck > 0 {
			_, err := c.sendLog.Commit(p.ack)
			if err != nil {
				c.Log("Error committing ack [%v] : [%v]", p.ack, err)
				c.stats.packetsDropped.Inc(1)
				continue
			}
		}

		// Handle: data flag (consume elements of the stream)
		if p.ctrls&PacketFlagData > 0 {
			_, _, head := c.recvLog.Refs()

			// Handle: Past segments.  Just drop
			if head.offset > p.offset+uint32(len(p.data)) {
				c.Log("Received past segment [%v, %v]", p.offset, p.offset+uint32(len(p.data)))
				c.stats.packetsDropped.Inc(1)
				continue
			}

			pending.Put(p.offset, p.data)
		}

		// consume the pending items
		for {
			k, v := pending.Min()
			if k == nil || v == nil {
				break
			}

			// Take a snapshot of the current receive stream offsets
			_, _, head := c.recvLog.Refs()

			// Handle: Future offset
			offset, data := k.(uint32), v.([]byte)
			if offset > head.offset {
				break
			}

			// Handle: Past offset
			pending.Remove(offset)
			if head.offset > offset+uint32(len(data)) {
				continue
			}

			// Handle: Write the valid elements of the segment.
			// c.Log("Segment received: %v, %v", p, data)
			c.recvLog.Write(data[head.offset-offset:])
		}
	}
}

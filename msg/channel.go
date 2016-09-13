package msg

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"math/rand"

	"github.com/emirpasic/gods/maps/treemap"
)

// Much of this was inspired by the following papers:
//
// https://tools.ietf.org/html/rfc793
// http://www.ietf.org/proceedings/44/I-D/draft-ietf-sigtran-reliable-udp-00.txt
// https://tools.ietf.org/html/rfc4987
//
// TODOS:
//   * Make open/close more reliable.
//   * Make open/close more secure.  (Ensure packets are properly addressed!)
//   * Move entityId to uuid.
//   * Move to varint encoding.
//   * Handle offset/ack overflow!!!
//   * Protect against offset flood.
//   * Atomic options.
//   * Drop packets when recv buffer is full!!!
//
var (
	ErrHandshakeFailed = errors.New("CHAN:HANDSHAKE")
)

// Error types
var (
	ErrChannelClosed   = errors.New("CHAN:CLOSED")
	ErrChannelFailure  = errors.New("CHAN:FAILURE")
	ErrChannelResponse = errors.New("CHAN:RESPONSE")
	ErrChannelTimeout  = errors.New("CHAN:TIMEOUT")
	ErrChannelExists   = errors.New("CHAN:EXISTS")
	ErrChannelUnknown  = errors.New("CHAN:UNKNONW")
)

const (
	ChannelOpening AtomicState = 1 << iota
	ChannelOpened
	ChannelClosing
	ChannelClosed
	ChannelFailure
)

const (
	defaultChannelRecvInSize   = 1024
	defaultChannelRecvLogSize  = 1 << 20 // 1024K
	defaultChannelSendLogSize  = 1 << 18 // 256K
	defaultChannelSendWait     = 100 * time.Millisecond
	defaultChannelRecvWait     = 20 * time.Millisecond
	defaultChannelAckTimeout   = 5 * time.Second
	defaultChannelWinTimeout   = 2 * time.Second
	defaultChannelCloseTimeout = 10 * time.Second
	defaultChannelMaxRetries   = 3
)

// A channel represents one side of an active sessin.
//
// *Implementations must be thread-safe*
//
type Channel interface {
	Routable
	io.Reader
	io.Writer
}

// Function to be called when configuring a channel.  The function will
// accept a mutable channel options object that the callee may update.
type ChannelOptionsHandler func(*ChannelOptions)

// Function to be called when state transitions occur.
type ChannelTransitionHandler func(Channel) error

// All the channel options
type ChannelOptions struct {

	// Whether or not to enable debug logging.
	Debug bool

	// Defines how many packets will be buffered before blocking
	RecvInSize int

	// Defines how many bytes will be buffered prior to being consumed (should always be greater than send buf)
	RecvLogSize int

	// Defines how many bytes will be buffered prior to being consumed (should always be greater than send buf)
	SendLogSize int

	// The duration to wait before trying to send data again (when none was available)
	SendWait time.Duration

	// The duration to wait before trying to fetch again (when none was avaiable)
	RecvWait time.Duration

	// The duration to wait for an ack before data is considered lost
	AckTimeout time.Duration

	// The duration to wait for an ack before data is considered lost
	WinTimeout time.Duration

	// The duration to wait before the close is aborted.  any pending data is considered lost.
	CloseTimeout time.Duration

	// The number of consecutive retries before the channel is tarnsitioned to a failure state.
	MaxRetries int

	// to be called when the channel has been initialized, and is in the process of being started
	OnInit ChannelTransitionHandler

	// to be called when the channel has encountered an unrecoverable error
	OnOpen ChannelTransitionHandler

	// to be called when the channel is closed (allows the release of external resources (ie ids, routing table))
	OnClose ChannelTransitionHandler

	// to be called when the channel is closed (allows the release of external resources (ie ids, routing table))
	OnFailure ChannelTransitionHandler

	// to be called when the channel produces an outgoing packet. (may block)
	OnData func(*Packet) error
}

// Returns the default options.
func defaultChannelOptions() *ChannelOptions {
	return &ChannelOptions{
		RecvInSize:   defaultChannelRecvInSize,
		RecvLogSize:  defaultChannelRecvLogSize,
		SendLogSize:  defaultChannelSendLogSize,
		SendWait:     defaultChannelSendWait,
		RecvWait:     defaultChannelRecvWait,
		AckTimeout:   defaultChannelAckTimeout,
		WinTimeout:   defaultChannelWinTimeout,
		CloseTimeout: defaultChannelCloseTimeout,
		MaxRetries:   defaultChannelMaxRetries,

		// state handlers
		OnInit:    func(c Channel) error { return nil },
		OnOpen:    func(c Channel) error { return nil },
		OnClose:   func(c Channel) error { return nil },
		OnFailure: func(c Channel) error { return nil },
		OnData:    func(p *Packet) error { return nil }}
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
//   * Senders currently implement timeout based retransmission.
//
//  Similar to TCP, this protocol ensures reliable, in-order delivery of data.
//  This also allows sessions to be resumed if errors occur. However, unlike
//  TCP, this does NOT attempt to solve the following problems:
//
//     * Flow/congestion control
//     * Message integrity
//
//  The class implements the following state machine:
//
//    init-->opening-->opened-->closing-->closed
//              |         |        |
//              |--------->-------->----->failure
//
//  The state machine is implemented using compare and swaps.
//  In other words, there is no guarantee of strong consistency
//  with respect to the actions taken with out of date knowledge.
//  This is done by design, as the need for strong consistency
//  lead to deadlock where an external action block transitions
//  indefinitely if they themselves are blocked or long running.
//  Consistency errors are avoided by synchronized access to any
//  shared data structures.
//
//  This class uses the following threading model:
//
//  thread1: (consumer read thread)
//     * Makes external facing calls (e.g. Read/Close)
//  thread2: (consumer write thread) [optional]
//     * Makes external facing calls (e.g. Write/Flush/Close)
//  thread3: (mux router thread)
//     * Spawns and routes packets to the thread.  Can also initiate a close.
//  thread4: (recv thread)
//     * Accepts incoming data from a different channel and pushes it to consumer thread.
//  thread5: (send thread)
//     * Accepts incoming data from the consumer and pushes it to the target channel.
//  thread6: (opening thread)
//     * Performs the opening handshake.  This is a shortlived thread.
//  thread7: (closing thread)
//     * Performs the closing handshake and closes all resources.  This is a shortlived thread.
//
// Channel Opening:
//     1. Transition to Opening.
//     2. Perform reliable handshake
//         * Initiator: send(open), recv(open,ack), send(ack)
//         * Listener:  recv(open), send(open,ack), recv(ack)
//     3. Transition to Opened.
//
// Channel Closing:
//     1. Transition to Closing
//     2. Perform reliable handshake
//         * Initiator: send(close), recv(close,ack), send(ack)
//         * Listener:  recv(close), send(close,ack), recv(ack)
//     3. Transition to Closed.
//
// *This object is thread safe.*
//
type channel struct {

	// the address of the session
	session Session

	// channel options.
	options *ChannelOptions

	// general channel statistics
	stats *ChannelStats

	// the state of the channel.
	state *AtomicState

	// receive buffers
	recvIn  chan *Packet
	recvOut *Stream

	// send buffers
	sendIn  *Stream
	sendOut func(p *Packet) error

	// event handlers
	onInit    ChannelTransitionHandler
	onOpen    ChannelTransitionHandler
	onClose   ChannelTransitionHandler
	onFailure ChannelTransitionHandler

	// the workers wait
	workers sync.WaitGroup
}

// Creates and returns a new channel
func newChannel(l EndPoint, r EndPoint, listening bool, opts ...ChannelOptionsHandler) *channel {
	// initialize the options.
	defaultOpts := defaultChannelOptions()
	for _, opt := range opts {
		opt(defaultOpts)
	}

	// defensively copy the options (this is to eliminate any reference to the options that the consumer may have)
	options := *defaultOpts

	// create the channel
	c := &channel{
		session:   NewChannelSession(l, r),
		options:   &options,
		stats:     NewChannelStats(l),
		state:     NewAtomicState(ChannelOpening),
		recvIn:    make(chan *Packet, options.RecvInSize),
		recvOut:   NewStream(options.RecvLogSize),
		sendIn:    NewStream(options.SendLogSize),
		sendOut:   options.OnData,
		onInit:    options.OnInit,
		onOpen:    options.OnOpen,
		onClose:   options.OnClose,
		onFailure: options.OnFailure}

	// call the init function. (before anything!)
	c.onInit(c)

	// kick off the workers
	c.workers.Add(3)
	go sendWorker(c)
	go recvWorker(c)
	go openWorker(c, listening)

	// finally, return it.
	return c
}

func (c *channel) Session() Session {
	return c.session
}

func (c *channel) Flush() error {
	return c.flush(365 * 24 * time.Hour)
}

// Reads data from the channel.  Blocks if data isn't available.
func (c *channel) Read(buf []byte) (int, error) {
	state := c.state.WaitUntil(ChannelOpened | ChannelClosed | ChannelFailure)
	if state.Is(ChannelClosed | ChannelFailure) {
		return 0, ErrChannelClosed
	}

	return c.recvOut.Read(buf)
}

// Writes the data to the channel.  Blocks if the underlying send buffer is full.
func (c *channel) Write(data []byte) (int, error) {
	state := c.state.WaitUntil(ChannelOpened | ChannelClosed | ChannelFailure)
	if state.Is(ChannelClosed | ChannelClosing | ChannelFailure) {
		return 0, ErrChannelClosed
	}

	return c.sendIn.Write(data)
}

// Closes the channel.  Returns an error if the channel is already closed.
func (c *channel) Close() error {
	c.state.WaitUntil(ChannelOpened | ChannelClosed | ChannelFailure)
	if !c.state.Transition(ChannelOpened, ChannelClosing) {
		return ErrChannelClosed
	}

	c.workers.Add(1)
	go closeWorker(c, nil)
	c.workers.Wait()
	return nil
}

// ** INTERNAL ONLY METHODS **

// Send pushes a message on the input channel.  (used for internal routing.)
func (c *channel) send(p *Packet) error {
	if !c.state.Is(ChannelOpening | ChannelOpened | ChannelClosing) {
		return ErrChannelClosed
	}

	c.recvIn <- p
	return nil
}

// Flushes the sendlog.
func (c *channel) flush(timeout time.Duration) error {
	state := c.state.WaitUntil(ChannelOpened | ChannelClosed | ChannelFailure)
	if state.Is(^ChannelOpened) {
		return ErrChannelClosed
	}

	_, _, head, _ := c.sendIn.Snapshot()

	start := time.Now()
	for {
		tail, _, _, closed := c.sendIn.Snapshot()
		if closed {
			return ErrChannelClosed
		}

		if tail.offset >= head.offset {
			return nil
		}

		if time.Since(start) >= timeout {
			return ErrChannelTimeout
		}

		time.Sleep(c.options.SendWait)
	}
}

// Logs a message, tagging it with the channel's local address.
func (c *channel) log(format string, vals ...interface{}) {
	if !c.options.Debug {
		return
	}

	log.Println(fmt.Sprintf("[%v] -- ", c.session) + fmt.Sprintf(format, vals...))
}

func openWorker(c *channel, listening bool) {
	defer c.workers.Done()

	var err error

	for i := 0; i < c.options.MaxRetries; i++ {
		if listening {
			err = openRecv(c)
		} else {
			err = openInit(c)
		}
		if err == nil {
			break
		}

		c.log("Error on open attempt [%v]: %v", i, err)
	}

	if err != nil {
		c.log("Failure to open channel")
		if c.state.Transition(ChannelOpening, ChannelFailure) {
			c.onFailure(c)
		}
	} else {
		c.log("Successfully opened channel")
		if c.state.Transition(ChannelOpening, ChannelOpened) {
			c.onOpen(c)
		}
	}
}

func closeWorker(c *channel, p *Packet) {
	defer c.workers.Done()

	var err error
	if p == nil {
		err = closeInit(c)
	} else {
		err = closeRecv(c, p)
	}

	c.sendIn.Close()
	c.recvOut.Close()

	if err != nil {
		c.log("Failure to close channel")
		if c.state.Transition(AnyAtomicState, ChannelFailure) {
			c.onFailure(c)
		}
	} else {
		c.log("Successfully closed channel")
		if c.state.Transition(ChannelClosing, ChannelClosed) {
			c.onClose(c)
		}
	}
}

func sendWorker(c *channel) {
	defer c.workers.Done()
	defer c.log("Send worker shutdown")

	// initialize the timeout values
	timeout := c.options.AckTimeout
	timeoutCnt := 0

	// track last ack received
	recvAck, _, _, _ := c.sendIn.Snapshot()

	// track last ack sent
	_, _, sendAck, _ := c.recvOut.Snapshot()

	// the packet buffer (initialized here so we don't continually recreate memory.)
	tmp := make([]byte, PacketMaxLength)
	for {
		state := c.state.WaitUntil(ChannelOpened | ChannelClosing | ChannelClosed | ChannelFailure)
		if !state.Is(ChannelOpened) {
			return
		}

		var err error

		// ** IMPORTANT ** Channel state can still change!  Need to lock
		// at places that can have external side effects, or at least be
		// able to detect state changes and handle appropriately.

		// let's see if we need to retransmit
		sendTail, sendCur, _, sendClosed := c.sendIn.Snapshot()
		if sendClosed {
			return
		}

		// if we received an ack recently, reset the timeout values
		if sendTail.offset > recvAck.offset {
			recvAck = sendTail
			timeout = c.options.AckTimeout
			timeoutCnt = 0
		}

		// let's see if we're in a timeout senario.
		if sendCur.offset > sendTail.offset && time.Since(sendTail.time) >= timeout {
			cur, prev, err := c.sendIn.Reset()
			if err != nil {
				return
			}

			c.stats.numResets.Inc(1)
			c.log("Ack timed out. Reset send log to [%v] from [%v]", cur.offset, prev.offset)

			// double the timeout (ie exponential backoff)
			timeout *= 2
			c.log("Ack timeout increased to [%v]", timeout)

			if timeoutCnt++; timeoutCnt >= c.options.MaxRetries {
				c.log("Failure! Too many timeouts.")
				c.state.Transition(AnyAtomicState, ChannelFailure)
				c.onFailure(c)
				return
			}

			continue
		}

		// start building the outgoing packet
		flags := PacketFlagNone

		// see if we should be sending a data packet.
		sendStart, num, err := c.sendIn.TryRead(tmp, false)
		if err != nil {
			return
		}

		// build the packet data.
		data := tmp[:num]
		if num > 0 {
			flags = flags | PacketFlagOffset
		}

		// see if we should be sending an ack.
		_, _, recvHead, _ := c.recvOut.Snapshot()
		if recvHead.offset > sendAck.offset || time.Since(sendAck.time) >= c.options.AckTimeout/2 {
			flags = flags | PacketFlagAck
			sendAck = NewRef(recvHead.offset)
		}

		// just sleep if nothing to do
		if flags == PacketFlagNone {
			time.Sleep(c.options.SendWait)
			continue
		}

		// this can block indefinitely (What should we do???)
		if err := c.sendOut(newPacket(c, flags, sendStart.offset, recvHead.offset, data)); err != nil {
			c.state.Transition(AnyAtomicState, ChannelFailure)
			c.onFailure(c)
			return
		}

		c.stats.packetsSent.Inc(1)
	}
}

func recvWorker(c *channel) {
	defer c.workers.Done()
	defer c.log("Recv worker shutdown")

	// we'll use a simple sorted tree map to track out of order segments
	pending := treemap.NewWith(OffsetComparator)
	for {
		// block until we can do something useful!
		state := c.state.WaitUntil(ChannelOpened | ChannelClosing | ChannelClosed | ChannelFailure)
		if !state.Is(ChannelOpened) {
			return
		}

		// ** IMPORTANT ** Channel state can still change!  Need to lock
		// at places that can have external side effects, or at least be
		// able to detect state changes and handle appropriately.

		// grab the next packet (cannot block as we need to evaluate state transitions)
		var p *Packet = nil
		select {
		case in, ok := <-c.recvIn:
			if !ok {
				return
			}

			p = in
		default:
			break
		}

		if p == nil {
			time.Sleep(c.options.RecvWait)
			continue
		}

		c.stats.packetsReceived.Inc(1)

		// Handle: close flag
		if p.ctrls&PacketFlagClose > 0 {
			if !c.state.Transition(ChannelOpened, ChannelClosing) {
				return
			}

			c.workers.Add(1)
			go closeWorker(c, p)
			return
		}

		// Handle: ack flag
		if p.ctrls&PacketFlagAck > 0 {
			_, err := c.sendIn.Commit(p.ack)
			switch err {
			case ErrStreamClosed:
				c.log("Error committing ack. Send log closed.")
				return
			case ErrStreamInvalidCommit:
				c.log("Error committing ack [%v] : [%v]", p.ack, err)
				c.stats.packetsDropped.Inc(1)
				continue
			}
		}

		// Handle: data flag (consume elements of the stream)
		if p.ctrls&PacketFlagOffset > 0 {
			pending.Put(p.offset, p.data)
		}

		// consume the pending items
		for {
			k, v := pending.Min()
			if k == nil || v == nil {
				break
			}

			// Take a snapshot of the current receive stream offsets
			_, _, head, _ := c.recvOut.Snapshot()

			// Handle: Future offset
			offset, data := k.(uint32), v.([]byte)
			if offset > head.offset {
				break
			}

			// Handle: Past offset
			pending.Remove(offset)
			if head.offset > offset+uint32(len(data)) {
				c.stats.packetsDropped.Inc(1)
				continue
			}

			// Handle: Write the valid elements of the segment.
			if _, err := c.recvOut.Write(data[head.offset-offset:]); err != nil {
				return
			}
		}
	}
}

// Performs initiator  open handshake: send(open), recv(open,ack), send(ack)
func openInit(c *channel) error {
	c.log("Initiating open request")

	var p *Packet
	var err error

	// generate a new offset for the handshake.
	offset := rand.Uint32()

	// Send: open
	c.log("Send (open)")
	if err = send(c, PacketFlagOpen, offset, 0, []byte{}); err != nil {
		return ErrHandshakeFailed
	}

	// Receive: open, ack
	c.log("Receive (open, ack)")
	p, err = recvOrTimeout(c, c.options.AckTimeout)
	if err != nil || p == nil {
		send(c, PacketFlagErr, 0, 0, []byte("AckTimeout"))
		return ErrChannelTimeout
	}

	if p.ctrls != (PacketFlagOpen|PacketFlagAck) || p.ack != offset {
		send(c, PacketFlagErr, 0, 0, []byte("Incorrect Ack"))
		return ErrHandshakeFailed
	}

	// Send: ack
	c.log("Send (ack)")
	if err = send(c, PacketFlagAck, 0, p.offset, []byte{}); err != nil {
		return ErrHandshakeFailed
	}

	return nil
}

// Performs receiver (ie listener) open handshake: recv(open), send(open,ack), recv(ack)
func openRecv(c *channel) error {
	c.log("Waiting for open request")

	// Receive: open
	c.log("Receive (open)")
	p, err := recvOrTimeout(c, c.options.AckTimeout)
	if err != nil {
		return ErrChannelTimeout
	}

	if p.ctrls != PacketFlagOpen {
		send(c, PacketFlagErr, 0, 0, []byte("Required OPEN flag"))
		return ErrHandshakeFailed
	}

	// Send: open,ack
	c.log("Sending (open,ack)")
	offset := rand.Uint32()
	if err := send(c, PacketFlagOpen|PacketFlagAck, offset, p.offset, []byte{}); err != nil {
		return ErrHandshakeFailed
	}

	// Receive: Ack
	c.log("Receive (ack)")
	p, err = recvOrTimeout(c, c.options.AckTimeout)
	if err != nil {
		send(c, PacketFlagErr, 0, 0, []byte("Ack Timeout"))
		return ErrHandshakeFailed
	}

	if p.ctrls != PacketFlagAck || p.ack != offset {
		send(c, PacketFlagErr, 0, 0, []byte("Incorrect offset"))
		return ErrHandshakeFailed
	}

	return nil
}

// Performs close handshake from initiator's perspective: send(close), recv(close, ack), send(ack)
func closeInit(c *channel) error {
	var p *Packet
	var err error

	// generate a new random offset for jhe handshake.
	offset := rand.Uint32()

	// Send: close
	if err = send(c, PacketFlagClose, offset, 0, []byte{}); err != nil {
		return ErrHandshakeFailed
	}

	// Receive: close, ack (drop any packets taht don't conform to that)
	for {
		p, err = recvOrTimeout(c, c.options.AckTimeout)
		if err != nil || p == nil {
			return ErrChannelTimeout
		}

		if p.ctrls == (PacketFlagClose|PacketFlagAck) && p.ack == offset {
			break
		}
	}

	// Send: ack
	if err = send(c, PacketFlagAck, 0, p.offset, []byte{}); err != nil {
		return ErrHandshakeFailed
	}

	return nil
}

// Performs receiver (ie listener) close handshake: recv(close), send(close,ack), recv(ack)
func closeRecv(c *channel, p *Packet) error {

	// Send: close ack
	offset := rand.Uint32()
	if err := send(c, PacketFlagClose|PacketFlagAck, offset, p.offset, []byte{}); err != nil {
		return ErrHandshakeFailed
	}

	// Receive: Ack
	p, err := recvOrTimeout(c, c.options.AckTimeout)
	if err != nil {
		send(c, PacketFlagErr, 0, 0, []byte("Ack timeout.  Aborting."))
		return ErrHandshakeFailed
	}

	if p.ctrls != PacketFlagAck || p.ack != offset {
		send(c, PacketFlagErr, 0, 0, []byte("Incorrect offset."))
		return ErrHandshakeFailed
	}

	return nil
}

func newPacket(c *channel, flags PacketFlags, offset uint32, ack uint32, data []byte) *Packet {
	local := c.session.LocalEndPoint()
	remote := c.session.RemoteEndPoint()

	return NewPacket(local.EntityId(), local.ChannelId(), remote.EntityId(), remote.ChannelId(), flags, offset, ack, 0, data)
}

func send(c *channel, flags PacketFlags, offset uint32, ack uint32, data []byte) error {
	return c.sendOut(newPacket(c, flags, offset, ack, data))
}

func recvOrTimeout(c *channel, timeout time.Duration) (*Packet, error) {
	timer := time.NewTimer(timeout)

	select {
	case <-timer.C:
		return nil, ErrChannelTimeout
	case p := <-c.recvIn:
		return p, nil
	}
}

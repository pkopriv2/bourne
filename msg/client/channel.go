package client

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"math/rand"

	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"

	"github.com/emirpasic/gods/maps/treemap"

	metrics "github.com/rcrowley/go-metrics"
)

// Much of this was inspired by the following papers:
//
// https://tools.ietf.org/html/rfc793
// http://www.ietf.org/proceedings/44/I-D/draft-ietf-sigtran-reliable-udp-00.txt
// https://tools.ietf.org/html/rfc4987
//
// TODOS:
//   * Better errors!
//   * Make open/close more reliable.
//   * Make open/close more secure.  (Ensure packets are properly addressed!)
//   * Move entityId to uuid.
//   * Move to varint encoding.
//   * Handle offset/verify overflow!!!
//   * Protect against offset flood.
//   * Atomic options.
//   * Drop packets when recv buffer is full!!!
//
var (
	ErrHandshakeFailed = errors.New("CHAN:HANDSHAKE")
	ErrChannelClosed   = errors.New("CHAN:CLOSED")
	ErrChannelFailure  = errors.New("CHAN:FAILURE")
	ErrChannelResponse = errors.New("CHAN:RESPONSE")
	ErrChannelTimeout  = errors.New("CHAN:TIMEOUT")
	ErrChannelExists   = errors.New("CHAN:EXISTS")
	ErrChannelUnknown  = errors.New("CHAN:UNKNOWN")
)

const (
	ChannelOpening AtomicState = 1 << iota
	ChannelOpened
	ChannelClosing
	ChannelClosed
	ChannelFailure
)

const (
	confChannelDebug        = "bourne.msg.channel.debug"
	confChannelRecvInSize   = "bourne.msg.channel.recv.in.size"
	confChannelRecvLogSize  = "bourne.msg.channel.recv.log.size"
	confChannelSendLogSize  = "bourne.msg.channel.send.log.size"
	confChannelSendWait     = "bourne.msg.channel.send.wait"
	confChannelRecvWait     = "bourne.msg.channel.recv.wait"
	confChannelAckTimeout   = "bourne.msg.channel.ack.timeout"
	confChannelCloseTimeout = "bourne.msg.channel.close.size"
	confChannelMaxRetries   = "bourne.msg.channel.max.retries"
)

const (
	defaultChannelRecvInSize   = 1024
	defaultChannelRecvLogSize  = 1 << 20 // 1024K
	defaultChannelSendLogSize  = 1 << 18 // 256K
	defaultChannelSendWait     = 100 * time.Millisecond
	defaultChannelRecvWait     = 20 * time.Millisecond
	defaultChannelAckTimeout   = 5 * time.Second
	defaultChannelCloseTimeout = 10 * time.Second
	defaultChannelMaxRetries   = 3
)

// A channel represents one side of an active sessin.
type Channel interface {
	Routable
	io.Reader
	io.Writer
}

// Channel options are used to configure a channel.
type ChannelOptions struct {
	Config utils.Config

	// lifecycle handlers
	OnInit  ChannelTransitionHandler
	OnOpen  ChannelTransitionHandler
	OnClose ChannelTransitionHandler
	OnFail  ChannelTransitionHandler

	// data handler
	OnData func(wire.Packet) error
}

type ChannelOptionsHandler func(*ChannelOptions)

type ChannelTransitionHandler func(Channel) error

func defaultChannelOptions() *ChannelOptions {
	return &ChannelOptions{
		Config: utils.NewEmptyConfig(),

		// state handlers
		OnInit:  func(c Channel) error { return nil },
		OnOpen:  func(c Channel) error { return nil },
		OnClose: func(c Channel) error { return nil },
		OnFail:  func(c Channel) error { return nil },
		OnData:  func(p wire.Packet) error { return nil }}
}

// will still need to define better statistics gathering,
// but this was an easy get up and going method.
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

func newChannelStats(endpoint wire.EndPoint) *ChannelStats {
	r := metrics.DefaultRegistry

	return &ChannelStats{
		packetsSent: metrics.NewRegisteredCounter(
			newChannelMetric(endpoint, "channel.wire.PacketsSent"), r),
		packetsReceived: metrics.NewRegisteredCounter(
			newChannelMetric(endpoint, "channel.wire.PacketsReceived"), r),
		packetsDropped: metrics.NewRegisteredCounter(
			newChannelMetric(endpoint, "channel.wire.PacketsDropped"), r),

		bytesSent: metrics.NewRegisteredCounter(
			newChannelMetric(endpoint, "channel.BytesSent"), r),
		bytesReceived: metrics.NewRegisteredCounter(
			newChannelMetric(endpoint, "channel.BytesReceived"), r),
		bytesDropped: metrics.NewRegisteredCounter(
			newChannelMetric(endpoint, "channel.BytesDropped"), r),
		bytesReset: metrics.NewRegisteredCounter(
			newChannelMetric(endpoint, "channel.BytesReset"), r),
		numResets: metrics.NewRegisteredCounter(
			newChannelMetric(endpoint, "channel.NumResets"), r)}
}

func newChannelMetric(endpoint wire.EndPoint, name string) string {
	return fmt.Sprintf("-- %+v --: %s", endpoint, name)
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
//         * Initiator: send(open), recv(open,verify), send(verify)
//         * Listener:  recv(open), send(open,verify), recv(verify)
//     3. Transition to Opened.
//
// Channel Closing:
//     1. Transition to Closing
//     2. Perform reliable handshake
//         * Initiator: send(close), recv(close,verify), send(verify)
//         * Listener:  recv(close), send(close,verify), recv(verify)
//     3. Transition to Closed.
//
// *This object is thread safe.*
//
type channel struct {

	// the endpointess of the session
	session wire.Address

	// general channel statistics
	stats *ChannelStats

	// the state of the channel.
	state *AtomicState

	recvIn  chan wire.Packet
	recvOut *Stream
	sendIn  *Stream
	sendOut func(p wire.Packet) error

	// event handlers
	onInit    ChannelTransitionHandler
	onOpen    ChannelTransitionHandler
	onClose   ChannelTransitionHandler
	onFailure ChannelTransitionHandler

	// increased logging
	debug bool

	// thread sleeps
	sendWait time.Duration
	recvWait time.Duration

	// timeouts
	ackTimeout   time.Duration
	closeTimeout time.Duration

	// number of attempts (ie open/close)
	maxRetries int

	// the workers wait
	workers sync.WaitGroup
}

// Creates and returns a new channel
func newChannel(l wire.EndPoint, r wire.EndPoint, listening bool, opts ...ChannelOptionsHandler) *channel {
	// initialize the options.
	defaultOpts := defaultChannelOptions()
	for _, opt := range opts {
		opt(defaultOpts)
	}

	// defensively copy the options (this is to eliminate any reference to the options that the consumer may have)
	options := *defaultOpts

	// preem
	recvInSize := options.Config.OptionalInt(confChannelRecvInSize, defaultChannelRecvInSize)
	recvLogSize := options.Config.OptionalInt(confChannelRecvLogSize, defaultChannelRecvLogSize)
	sendLogSize := options.Config.OptionalInt(confChannelSendLogSize, defaultChannelSendLogSize)

	// create the channel
	c := &channel{
		session: wire.NewRemoteAddress(l, r),
		stats:   newChannelStats(l),
		state:   NewAtomicState(ChannelOpening),

		recvIn:  make(chan wire.Packet, recvInSize),
		recvOut: NewStream(recvLogSize),
		sendIn:  NewStream(sendLogSize),
		sendOut: options.OnData,

		onInit:    options.OnInit,
		onOpen:    options.OnOpen,
		onClose:   options.OnClose,
		onFailure: options.OnFail,

		debug:        options.Config.OptionalBool(confChannelDebug, false),
		sendWait:     options.Config.OptionalDuration(confChannelSendWait, defaultChannelSendWait),
		recvWait:     options.Config.OptionalDuration(confChannelRecvWait, defaultChannelRecvWait),
		ackTimeout:   options.Config.OptionalDuration(confChannelAckTimeout, defaultChannelAckTimeout),
		closeTimeout: options.Config.OptionalDuration(confChannelCloseTimeout, defaultChannelCloseTimeout),
		maxRetries:   options.Config.OptionalInt(confChannelMaxRetries, defaultChannelMaxRetries)}

	// call the init function.
	c.onInit(c)

	// kick off the workers
	c.workers.Add(3)
	go sendWorker(c)
	go recvWorker(c)
	go openWorker(c, listening)

	// finally, return it.
	return c
}

func (c *channel) Address() wire.Address {
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
func (c *channel) send(p wire.Packet) error {
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

		time.Sleep(c.sendWait)
	}
}

// Logs a message, tagging it with the channel's local endpointess.
func (c *channel) log(format string, vals ...interface{}) {
	if !c.debug {
		return
	}

	log.Println(fmt.Sprintf("[%v] -- ", c.session) + fmt.Sprintf(format, vals...))
}

func openWorker(c *channel, listening bool) {
	defer c.workers.Done()

	var err error

	for i := 0; i < c.maxRetries; i++ {
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

func closeWorker(c *channel, p wire.Packet) {
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
	timeout := c.ackTimeout
	timeoutCnt := 0

	// track last verify received
	recvAck, _, _, _ := c.sendIn.Snapshot()

	// track last verify sent
	_, _, sendAck, _ := c.recvOut.Snapshot()

	// the wire.Packet buffer (initialized here so we don't continually recreate memory.)
	tmp := make([]byte, wire.PacketMaxDataLength)
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

		// if we received an verify recently, reset the timeout values
		if sendTail.offset > recvAck.offset {
			recvAck = sendTail
			timeout = c.ackTimeout
			timeoutCnt = 0
		}

		// let's see if we're in a timeout senario.
		if sendCur.offset > sendTail.offset && time.Since(sendTail.time) >= timeout {
			cur, prev, err := c.sendIn.Reset()
			if err != nil {
				return
			}

			c.stats.numResets.Inc(1)
			c.log("Verify timed out. Reset send log to [%v] from [%v]", cur.offset, prev.offset)

			// double the timeout (ie exponential backoff)
			timeout *= 2
			c.log("Verify timeout increased to [%v]", timeout)

			if timeoutCnt++; timeoutCnt >= c.maxRetries {
				c.log("Failure! Too many timeouts.")
				c.state.Transition(AnyAtomicState, ChannelFailure)
				c.onFailure(c)
				return
			}

			continue
		}

		// start building the outgoing wire.Packet
		flags := wire.FlagNone

		// see if we should be sending a data wire.Packet.
		sendStart, num, err := c.sendIn.TryRead(tmp, false)
		if err != nil {
			return
		}

		// build the wire.Packet data.
		data := tmp[:num]
		if num > 0 {
			flags = flags | wire.FlagOffset
		}

		// see if we should be sending an verify.
		_, _, recvHead, _ := c.recvOut.Snapshot()
		if recvHead.offset > sendAck.offset || time.Since(sendAck.time) >= c.ackTimeout/2 {
			flags = flags | wire.FlagVerify
			sendAck = NewRef(recvHead.offset)
		}

		// just sleep if nothing to do
		if flags == wire.FlagNone {
			time.Sleep(c.sendWait)
			continue
		}

		// this can block indefinitely (What should we do???)
		if err := c.sendOut(wire.NewStandardPacket(c.Address(), flags, sendStart.offset, recvHead.offset, data)); err != nil {
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

		// grab the next wire.Packet (cannot block as we need to evaluate state transitions)
		var p wire.Packet = nil
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
			time.Sleep(c.recvWait)
			continue
		}

		c.stats.packetsReceived.Inc(1)

		// Handle: close flag
		if p.HasFlag(wire.FlagClose) {
			if !c.state.Transition(ChannelOpened, ChannelClosing) {
				return
			}

			c.workers.Add(1)
			go closeWorker(c, p)
			return
		}

		// Handle: verify flag
		if p.HasFlag(wire.FlagVerify) {
			_, err := c.sendIn.Commit(p.Verify())
			switch err {
			case ErrStreamClosed:
				c.log("Error committing verify. Send log closed.")
				return
			case ErrStreamInvalidCommit:
				c.log("Error committing verify [%v] : [%v]", p.Verify(), err)
				c.stats.packetsDropped.Inc(1)
				continue
			}
		}

		// Handle: data flag (consume elements of the stream)
		if p.HasFlag(FlagOffset) {
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
				c.stats.wire.PacketsDropped.Inc(1)
				continue
			}

			// Handle: Write the valid elements of the segment.
			if _, err := c.recvOut.Write(data[head.offset-offset:]); err != nil {
				return
			}
		}
	}
}

// Performs initiator  open handshake: send(open), recv(open,verify), send(verify)
func openInit(c *channel) error {
	c.log("Initiating open request")

	var p wire.Packet
	var err error

	// generate a new offset for the handshake.
	offset := rand.Uint32()

	// Send: open
	c.log("Send (open)")
	if err = send(c, wire.FlagOpen, offset, 0, []byte{}); err != nil {
		return ErrHandshakeFailed
	}

	// Receive: open, verify
	c.log("Receive (open, verify)")
	p, err = recvOrTimeout(c, c.ackTimeout)
	if err != nil || p == nil {
		send(c, wire.FlagErr, 0, 0, []byte("AckTimeout"))
		return ErrChannelTimeout
	}

	if p.flags != (wire.FlagOpen|wire.FlagVerify) || p.verify != offset {
		send(c, wire.FlagErr, 0, 0, []byte("Incorrect verify"))
		return ErrHandshakeFailed
	}

	// Send: verify
	c.log("Send (verify)")
	if err = send(c, wire.FlagVerify, 0, p.offset, []byte{}); err != nil {
		return ErrHandshakeFailed
	}

	return nil
}

// Performs receiver (ie listener) open handshake: recv(open), send(open,verify), recv(verify)
func openRecv(c *channel) error {
	c.log("Waiting for open request")

	// Receive: open
	c.log("Receive (open)")
	p, err := recvOrTimeout(c, c.ackTimeout)
	if err != nil {
		return ErrChannelTimeout
	}

	if p.flags != wire.FlagOpen {
		send(c, wire.FlagErr, 0, 0, []byte("Required OPEN flag"))
		return ErrHandshakeFailed
	}

	// Send: open,verify
	c.log("Sending (open,verify)")
	offset := rand.Uint32()
	if err := send(c, wire.FlagOpen|wire.FlagVerify, offset, p.offset, []byte{}); err != nil {
		return ErrHandshakeFailed
	}

	// Receive: verify
	c.log("Receive (verify)")
	p, err = recvOrTimeout(c, c.ackTimeout)
	if err != nil {
		send(c, wire.FlagErr, 0, 0, []byte("verify Timeout"))
		return ErrHandshakeFailed
	}

	if p.flags != wire.FlagVerify || p.verify != offset {
		send(c, wire.FlagErr, 0, 0, []byte("Incorrect offset"))
		return ErrHandshakeFailed
	}

	return nil
}

// Performs close handshake from initiator's perspective: send(close), recv(close, verify), send(verify)
func closeInit(c *channel) error {
	var p wire.Packet
	var err error

	// generate a new random offset for jhe handshake.
	offset := rand.Uint32()

	// Send: close
	if err = send(c, wire.FlagClose, offset, 0, []byte{}); err != nil {
		return ErrHandshakeFailed
	}

	// Receive: close, verify (drop any wire.Packets taht don't conform to that)
	for {
		p, err = recvOrTimeout(c, c.ackTimeout)
		if err != nil || p == nil {
			return ErrChannelTimeout
		}

		if p.flags == (wire.FlagClose|wire.FlagVerify) && p.verify == offset {
			break
		}
	}

	// Send: verify
	if err = send(c, wire.FlagVerify, 0, p.offset, []byte{}); err != nil {
		return ErrHandshakeFailed
	}

	return nil
}

// Performs receiver (ie listener) close handshake: recv(close), send(close,verify), recv(verify)
func closeRecv(c *channel, p wire.Packet) error {

	// Send: close verify
	offset := rand.Uint32()
	if err := send(c, wire.FlagClose|wire.FlagVerify, offset, p.offset, []byte{}); err != nil {
		return ErrHandshakeFailed
	}

	// Receive: verify
	p, err := recvOrTimeout(c, c.ackTimeout)
	if err != nil {
		send(c, wire.FlagErr, 0, 0, []byte("verify timeout.  Aborting."))
		return ErrHandshakeFailed
	}

	if p.flags != wire.FlagVerify || p.verify != offset {
		send(c, wire.FlagErr, 0, 0, []byte("Incorrect offset."))
		return ErrHandshakeFailed
	}

	return nil
}

func send(c *channel, flags wire.Flags, offset uint32, verify uint32, data []byte) error {
	return c.sendOut(wire.NewPacket(c.session.Local(), c.session.Remote(), flags, offset, verify, data))
}

func recvOrTimeout(c *channel, timeout time.Duration) (wire.Packet, error) {
	timer := time.NewTimer(timeout)

	select {
	case <-timer.C:
		return nil, ErrChannelTimeout
	case p := <-c.recvIn:
		return p, nil
	}
}

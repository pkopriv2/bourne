package client

import (
	"fmt"
	"io"
	"log"
	"time"

	"math/rand"

	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"

	metrics "github.com/rcrowley/go-metrics"
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
//   * Close streams independently!
//   * Protect against offset flood.
//   * Drop packets when recv buffer is full!!!
//   * Better concurrency model!
//

// An active channel represents one side of a conversation between two entities.
//
//   * A channel reprents a full-duplex stream abstraction.
//
//   * A channel may be thought of as two send streams, with one sender on each
//     side of the session.
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
//     2. Perform close handshake
//         * Initiator: send(close), recv(close,verify), send(verify)
//         * Listener:  recv(close), send(close,verify), recv(verify)
//     3. Transition to Closed.
//
// Channel Failure:
//     1. Transition to Failure
//
// *This object is thread safe.*
//
const (
	ChannelOpeningErrorCode = 100
	ChannelClosingErrorCode = 101
	ChannelTimeoutErrorCode = 102
)

var (
	NewOpeningError = wire.NewProtocolErrorFamily(ChannelOpeningErrorCode)
	NewClosingError = wire.NewProtocolErrorFamily(ChannelOpeningErrorCode)
	NewTimeoutError = wire.NewProtocolErrorFamily(ChannelOpeningErrorCode)
)

const (
	ChannelInit = iota
	ChannelOpening
	ChannelOpened
	ChannelClosing
	ChannelClosed
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
	defaultChannelRecvInSize        = 1024
	defaultChannelRecvLogSize       = 1 << 20 // 1024K
	defaultChannelSendLogSize       = 1 << 18 // 256K
	defaultChannelSendWait          = 100 * time.Millisecond
	defaultChannelRecvWait          = 20 * time.Millisecond
	defaultChannelAckTimeout        = 5 * time.Second
	defaultChannelCloseTimeout      = 10 * time.Second
	defaultChannelMaxRetries        = 3
)

type Channel interface {
	Routable
	io.Reader
	io.Writer
}

type channelConfig struct {
	debug bool

	recvMax int

	ackTimeout   time.Duration
	closeTimeout time.Duration
	maxRetries   int
}

type ChannelOptions struct {
	Config utils.Config

	// lifecycle handlers
	OnInit  ChannelTransitionHandler
	OnOpen  ChannelTransitionHandler
	OnClose ChannelTransitionHandler
	OnFail  ChannelTransitionHandler
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
		OnFail:  func(c Channel) error { return nil }}
}

type channelComms struct {
	recvOut       chan []byte
	recvBuffer    chan []byte
	recvAssembler chan wire.SegmentMessage
	recvIn        chan wire.Packet

	sendIn        chan wire.Packet
	sendBuffer    chan []byte
	sendAssembler chan wire.SegmentMessage
	sendOut       chan []byte

	ack chan uint64

	onInit    ChannelTransitionHandler
	onOpen    ChannelTransitionHandler
	onClose   ChannelTransitionHandler
	onFailure ChannelTransitionHandler
}

type channel struct {

	// the complete address of the channel
	route wire.Route

	// general channel statistics
	stats *ChannelStats

	// the channel state machine
	machine utils.StateMachine

	// channel communications
	comms *channelComms

	// config
	config *channelConfig
}

// Creates and returns a new channel
func newChannel(route wire.Route, listening bool, opts ...ChannelOptionsHandler) *channel {
	// initialize the options.
	defaultOpts := defaultChannelOptions()
	for _, opt := range opts {
		opt(defaultOpts)
	}

	// defensively copy the options (this is to eliminate any reference to
	// the options that the consumer may have)
	options := *defaultOpts

	config := &channelConfig{
		debug:        options.Config.OptionalBool(confChannelDebug, false),
		ackTimeout:   options.Config.OptionalDuration(confChannelAckTimeout, defaultChannelAckTimeout),
		closeTimeout: options.Config.OptionalDuration(confChannelCloseTimeout, defaultChannelCloseTimeout),
		maxRetries:   options.Config.OptionalInt(confChannelMaxRetries, defaultChannelMaxRetries)}

	// create the channel channelComms
	comms := &channelComms{}

	// initialize the channel
	c := &channel{
		route:  route,
		stats:  newChannelStats(route.Src()),
		comms:  comms,
		config: config}

	// build the statemachine
	factory := utils.BuildStateMachine()
	// factory.AddState(ChannelInit, NewInitWorker(c))
	// factory.AddState(ChannelOpening, NewOpeningWorker(c, listening))
	// factory.AddState(ChannelOpened, NewRecvWorker(c), NewRecvAssembleWorker(c), NewRecvBufferWorker(c))

	// ugly....
	c.machine = factory.Start(ChannelInit)

	// finally, return it.
	return c
}

func (c *channel) Route() wire.Route {
	return c.route
}

// func (c *channel) Flush() error {
// return c.flush(365 * 24 * time.Hour)
// }

// Reads data from the channel.  Blocks if data isn't available.
func (c *channel) Read(buf []byte) (int, error) {
	return 0, nil
	// state := c.state.WaitUntil(ChannelOpened | ChannelClosed | ChannelFailure)
	// if state.Is(ChannelClosed | ChannelFailure) {
	// return 0, ErrChannelClosed
	// }
	//
	// return c.recvOut.Read(buf)
}

// Writes the data to the channel.  Blocks if the underlying send buffer is full.
func (c *channel) Write(data []byte) (int, error) {
	return 0, nil
	// state := c.state.WaitUntil(ChannelOpened | ChannelClosed | ChannelFailure)
	// if state.Is(ChannelClosed | ChannelClosing | ChannelFailure) {
	// return 0, ErrChannelClosed
	// }
	//
	// return c.sendIn.Write(data)
}

// Closes the channel.  Returns an error if the channel is already closed.
func (c *channel) Close() error {
	return nil
}

// ** INTERNAL ONLY METHODS **
func (c *channel) fail(reason error) {
	control, err := c.machine.Control()
	if err != nil {
		return
	}

	control.Fail(reason)
}

// Send pushes a message on the input channel.  (used for internal routing.)
func (c *channel) send(p wire.Packet) error {
	// c.recvIn <- p
	return nil
}

func NewInitWorker(channel *channel) func(utils.StateController) {
	return func(state utils.StateController) {
		channel.comms.onInit(channel)
		state.Next(ChannelOpening)
	}
}

func NewRecvBufferWorker(channel *channel) func(utils.StateController) {
	return func(state utils.StateController) {
	}
}

func NewRecvWorker(channel *channel) func(utils.StateController) {
	return func(state utils.StateController) {
		for {
			var p wire.Packet
			select {
			case <-state.Done():
				return
			case p = <-channel.comms.recvIn:
			}

			// Handle: close
			if close := p.Close(); close != nil {
				state.Next(ChannelClosing)
				return
			}

			// Handle: error
			if err := p.Error(); err != nil {
				state.Fail(err)
				return
			}

			// Handle: verify message
			if verify := p.Verify(); verify != nil {
				select {
				case <-state.Done():
					return
				case channel.comms.ack <- verify.Val():
				}
			}

			// Handle: segment
			if segment := p.Segment(); segment != nil {
				select {
				case <-state.Done():
					return
				case channel.comms.recvAssembler <- segment:
				}
			}
		}
	}
}

func NewSendBufferWorker(channel *channel) func(utils.StateController) {
	return func(state utils.StateController) {
	}
}

func NewSendWorker(channel *channel) func(utils.StateController) {
	return func(state utils.StateController) {
	}
}

func NewOpeningWorker(channel *channel, listening bool) func(utils.StateController) {
	return func(state utils.StateController) {
		var err error

		for i := 0; i < channel.config.maxRetries; i++ {
			if listening {
				// err = openRecv(channel)
			} else {
				// err = openInit(channel)
			}

			if err == nil {
				state.Next(ChannelOpened)
				return
			}
		}

		state.Fail(err)
	}
}

// func NewClosingWorker(channel *channel) func(utils.StateController) {
// return func(state utils.StateController) {
// var err error
//
// if p == nil {
// err = closeInit(channel)
// } else {
// err = closeRecv(c, p)
// }
//
// c.sendIn.Close()
// c.recvOut.Close()
//
// if err != nil {
// c.Fail(err)
// return
// }
//
// c.log("Successfully closed channel")
// c.Next(ChannelClosed)
// }
// }

// Logs a message, tagging it with the channel's local endpointess.
func (c *channel) log(format string, vals ...interface{}) {
	if !c.config.debug {
		return
	}

	log.Println(fmt.Sprintf("[%v] -- ", c.route) + fmt.Sprintf(format, vals...))
}

// Performs open handshake: send(open), recv(open,verify), send(verify)
func openInit(c *channel) error {
	c.log("Initiating open request")

	var p wire.Packet
	var err error

	// generate a new offset for the handshake.
	offset := uint64(rand.Uint32())

	// Send: open
	if err = send(c, wire.BuildPacket(c.route).SetOpen(offset).Build()); err != nil {
		return NewOpeningError("Failed: send(open)")
	}

	// Receive: open, verify
	p, err = recvOrTimeout(c, c.config.ackTimeout)
	if err != nil || p == nil {
		ret := NewOpeningError("Failed: recv(open, verify)")
		send(c, wire.BuildPacket(c.route).SetError(ret).Build())
		return ret
	}

	open, verify := p.Open(), p.Verify()
	if open == nil || verify == nil {
		ret := NewOpeningError("Failed: recv(open, verify): Missing open or verify")
		send(c, wire.BuildPacket(c.route).SetError(ret).Build())
		return ret
	}

	if verify.Val() != offset {
		ret := NewOpeningError(fmt.Sprintf("Failed: receive(open, verify): Incorrect verify [%v]", verify.Val()))
		send(c, wire.BuildPacket(c.route).SetError(ret).Build())
		return ret
	}

	// Send: verify
	c.log("Send (verify)")
	if err = send(c, wire.BuildPacket(c.route).SetVerify(open.Val()).Build()); err != nil {
		ret := NewOpeningError("Failed: send(verify)")
		send(c, wire.BuildPacket(c.route).SetError(ret).Build())
		return ret
	}

	return nil
}

// Performs receiver (ie listener) open handshake: recv(open), send(open,verify), recv(verify)
func openRecv(c *channel) error {
	c.log("Waiting for open request")

	// Receive: open
	c.log("Receive (open)")
	p, err := recvOrTimeout(c, c.config.ackTimeout)
	if err != nil {
		return NewOpeningError("Failed: recv(open)")
	}

	open := p.Open()
	if open == nil {
		ret := NewOpeningError("Failed: recv(open): Missing open message.")
		send(c, wire.BuildPacket(c.route).SetError(ret).Build())
		return ret
	}

	// Send: open,verify
	c.log("Sending (open,verify)")
	offset := uint64(rand.Uint32())
	if err := send(c, wire.BuildPacket(c.route).SetVerify(open.Val()).SetOpen(offset).Build()); err != nil {
		ret := NewOpeningError("Failed: send(open,verify)")
		send(c, wire.BuildPacket(c.route).SetError(ret).Build())
		return ret
	}

	// Receive: verify
	c.log("Receive (verify)")
	p, err = recvOrTimeout(c, c.config.ackTimeout)
	if err != nil {
		ret := NewOpeningError("Failed: recv(verify)")
		send(c, wire.BuildPacket(c.route).SetError(ret).Build())
		return ret
	}

	verify := p.Verify()
	if verify == nil || verify.Val() != offset {
		ret := NewOpeningError("Failed: recv(verify): Missing or incorrect verify")
		send(c, wire.BuildPacket(c.route).SetError(ret).Build())
		return ret
	}

	return nil
}

// Performs close handshake from initiator's perspective: send(close), recv(close, verify), send(verify)
func closeInit(c *channel) error {
	var err error

	// generate a new random value for the handshake.
	offset := uint64(rand.Uint32())

	// Send: close
	if err = send(c, wire.BuildPacket(c.route).SetClose(offset).Build()); err != nil {
		return NewClosingError("Failed: send(close)")
	}

	// Receive: close, verify (drop any non close packets)
	var p wire.Packet
	for {
		p, err = recvOrTimeout(c, c.config.ackTimeout)
		if err != nil || p == nil {
			return NewClosingError("Timeout: recv(close,verify)")
		}

		close, verify := p.Close(), p.Verify()
		if close == nil || verify == nil {
			continue
		}

		if verify.Val() == offset {
			break
		}
	}

	// Send: verify
	if err = send(c, wire.BuildPacket(c.route).SetClose(p.Close().Val()).Build()); err != nil {
		return NewClosingError("Failed: verify(close)")
	}

	return nil
}

// Performs receiver (ie listener) close handshake: recv(close), send(close,verify), recv(verify)
func closeRecv(c *channel, p wire.Packet) error {

	// Send: close verify
	offset := uint64(rand.Uint32())
	if err := send(c, wire.BuildPacket(c.route).SetClose(offset).SetVerify(p.Close().Val()).Build()); err != nil {
		return NewClosingError("Failed: send(close,verify)")
	}

	// Receive: verify
	p, err := recvOrTimeout(c, c.config.ackTimeout)
	if err != nil {
		return NewClosingError("Failed: receive(verify)")
	}

	if verify := p.Verify(); verify == nil || verify.Val() != offset {
		return NewClosingError("Failed: receive(verify). Incorrect value")
	}

	return nil
}

func recvOrTimeout(c *channel, timeout time.Duration) (wire.Packet, error) {
	timer := time.NewTimer(timeout)

	select {
	case <-timer.C:
		return nil, NewTimeoutError("Timeout")
	case p := <-c.comms.recvIn:
		return p, nil
	}
}

func send(c *channel, p wire.Packet) error {
	return nil
	// return c.sendOut(p)
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

func newChannelStats(endpoint wire.Address) *ChannelStats {
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

func newChannelMetric(endpoint wire.Address, name string) string {
	return fmt.Sprintf("-- %+v --: %s", endpoint, name)
}

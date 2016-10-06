package client

import (
	"io"
	"time"

	"github.com/pkopriv2/bourne/msg/core"
	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
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

// An active tunnel represents one side of a conversation between two entities.
//
//   * A tunnel reprents a full-duplex stream abstraction.
//
//   * A tunnel may be thought of as two send streams, with one sender on each
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
//
//          |-->openRecv--|           |-->closeRecv--|
//    init--|             |-->opened--|              |-->closed
//          |-->openInit--|           |-->closeInit--|
//
//
//
// Each state implements a separate "machine" or alternatively, a separate
// communications network.
//
//
// Tunnel Opening:
//     1. Transition to Opening.
//     2. Perform reliable handshake
//         * Initiator: send(open), recv(open,verify), send(verify)
//         * Listener:  recv(open), send(open,verify), recv(verify)
//     3. Transition to Opened.
//
// Tunnel Closing:
//     1. Transition to Closing
//     2. Perform close handshake
//         * Initiator: send(close), recv(close,verify), send(verify)
//         * Listener:  recv(close), send(close,verify), recv(verify)
//     3. Transition to Closed.
//
type Tunnel interface {
	io.Closer
	io.Reader
	io.Writer
}

// Errors
const (
	TunnelOpeningErrorCode = 100
	TunnelClosingErrorCode = 101
	TunnelTimeoutErrorCode = 102
)

var (
	NewOpeningError = wire.NewProtocolErrorFamily(TunnelOpeningErrorCode)
	NewClosingError = wire.NewProtocolErrorFamily(TunnelClosingErrorCode)
	NewTimeoutError = wire.NewProtocolErrorFamily(TunnelTimeoutErrorCode)
)

// Tunnel States
const (
	TunnelInit = iota
	TunnelOpeningInit
	TunnelOpeningRecv
	TunnelOpened
	TunnelClosingInit
	TunnelClosingRecv
	TunnelClosed
)

// Config
const (
	confTunnelDebug          = "bourne.msg.client.tunnel.debug"
	confTunnelBuffererLimit  = "bourne.msg.client.tunnel.bufferer.limit"
	confTunnelSenderLimit    = "bourne.msg.client.tunnel.sender.limit"
	confTunnelVerifyTimeout  = "bourne.msg.client.tunnel.verify.timeout"
	confTunnelSendTimeout    = "bourne.msg.client.tunnel.send.timeout"
	confTunnelRecvTimeout    = "bourne.msg.client.tunnel.recv.timeout"
	confTunnelMaxRetries     = "bourne.msg.client.tunnel.max.retries"
)

const (
	defaultTunnelBuffererLimit  = 1 << 20
	defaultTunnelSenderLimit    = 1 << 18
	defaultTunnelVerifyTimeout  = 5 * time.Second
	defaultTunnelSendTimeout    = 5 * time.Second
	defaultTunnelRecvTimeout    = 5 * time.Second
	defaultTunnelMaxRetries     = 3
)

type tunnelConfig struct {
	Debug          bool
	AssemblerLimit int
	BuffererLimit  int
	SenderLimit    int
	VerifyTimeout  time.Duration
	RecvTimeout    time.Duration
	SendTimeout    time.Duration
	MaxRetries     int
}

func newTunnelConfig(conf utils.Config) *tunnelConfig {
	return &tunnelConfig{
		Debug:          conf.OptionalBool(confTunnelDebug, false),
		AssemblerLimit: conf.OptionalInt(confTunnelAssemblerLimit, defaultTunnelAssemblerLimit),
		BuffererLimit:  conf.OptionalInt(confTunnelBuffererLimit, defaultTunnelBuffererLimit),
		SenderLimit:    conf.OptionalInt(confTunnelSenderLimit, defaultTunnelSenderLimit),
		VerifyTimeout:  conf.OptionalDuration(confTunnelVerifyTimeout, defaultTunnelVerifyTimeout),
		RecvTimeout:    conf.OptionalDuration(confTunnelRecvTimeout, defaultTunnelRecvTimeout),
		SendTimeout:    conf.OptionalDuration(confTunnelSendTimeout, defaultTunnelSendTimeout),
		MaxRetries:     conf.OptionalInt(confTunnelMaxRetries, defaultTunnelMaxRetries)}
}

// the general dependency injector
type tunnelEnv struct {
	logger utils.Logger
	config *tunnelConfig
}

func newTunnelEnv(config utils.Config) *tunnelEnv {
	return &tunnelEnv{logger: utils.NewStandardLogger(config), config: newTunnelConfig(config)}
}

func recvOrTimeout(e *tunnelEnv, in <-chan wire.Packet) (wire.Packet, error) {
	timer := time.NewTimer(e.config.RecvTimeout)

	select {
	case <-timer.C:
		return nil, NewTimeoutError("Timeout")
	case p := <-in:
		return p, nil
	}
}

func sendOrTimeout(e *tunnelEnv, out chan<- wire.Packet, p wire.Packet) error {
	timer := time.NewTimer(e.config.SendTimeout)

	select {
	case <-timer.C:
		return NewTimeoutError("Timeout")
	case out <- p:
		return nil
	}
}

// complete listing of tunnel channels.
type tunnelDriver struct {
	mainIn    chan wire.Packet
	mainOut   chan<- wire.Packet
	mainErr   chan error
	mainClose chan struct{}

	buffererIn   chan []byte
	assemblerIn  chan wire.SegmentMessage
	sendVerifier chan wire.NumMessage
	recvVerifier chan wire.NumMessage
}

func newTunnelChannels(sendMain chan<- wire.Packet) *tunnelDriver {
	return &tunnelDriver{
		mainOut:      sendMain,
		mainIn:       make(chan wire.Packet),
		buffererIn:   make(chan []byte),
		assemblerIn:  make(chan wire.SegmentMessage),
		sendVerifier: make(chan wire.NumMessage),
		recvVerifier: make(chan wire.NumMessage)}
}

// the main tunnel abstraction
type tunnel struct {
	route    wire.Route
	env      *tunnelEnv
	channels *tunnelDriver
	machine  utils.StateMachine

	streamRecv *Stream
	streamSend *Stream
}


func NewTunnel(sub *TunnelSubscription) Tunnel {
	return nil

	// var env tunnelEnv
	// // initialize the environment
	// // env := newTunnelEnv(sub.)
//
	// // initialize all the channels
	// channels := newTunnelChannels(mainSend)
//
	// // opening workers
	// openerInit := NewOpenerInit(route, env, channels)
	// openerRecv := NewOpenerRecv(route, env, channels)
//
	// // closing workers
	// closerInit := NewCloserInit(route, env, channels)
	// closerRecv := NewCloserRecv(route, env, channels)
//
	// // opened workers
	// streamSend, sendMain := NewSendMain(route, env, channels)
	// recvMain := NewRecvMain(env, channels)
	// recvAssembler := NewRecvAssembler(env, channels)
	// streamRecv, recvBuff := NewRecvBuffer(env, channels)
//
	// // build the machine
	// builder := utils.BuildStateMachine()
	// builder.AddState(TunnelOpeningInit, openerInit)
	// builder.AddState(TunnelOpeningRecv, openerRecv)
	// builder.AddState(TunnelOpened, sendMain, recvMain, recvAssembler, recvBuff)
	// builder.AddState(TunnelClosingInit, closerInit)
	// builder.AddState(TunnelClosingRecv, closerRecv)
//
	// // start the machine
	// var machine utils.StateMachine
	// if options.Listening {
		// machine = builder.Start(TunnelOpeningInit)
	// } else {
		// machine = builder.Start(TunnelOpeningRecv)
	// }
//
	// return &tunnel{
		// route:      route,
		// env:        env,
		// machine:    machine,
		// channels:   channels,
		// streamRecv: streamRecv,
		// streamSend: streamSend}
}

func (t *tunnel) Route() wire.Route {
	return t.route
}

func (t *tunnel) Read(p []byte) (n int, err error) {
	return t.streamRecv.Read(p)
}

func (t *tunnel) Write(p []byte) (n int, err error) {
	return t.streamSend.Write(p)
}

func (t *tunnel) Close() error {
	control := t.machine.Control()

	select {
	case control.Transition() <- utils.State(TunnelClosingInit):
		return nil
	case err := <-control.Wait():
		return err
	}
}

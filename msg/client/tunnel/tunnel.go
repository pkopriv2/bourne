package tunnel

import (
	"fmt"
	"io"
	"log"
	"time"

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
	wire.Routable
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
	confTunnelAssemblerLimit = "bourne.msg.client.tunnel.assembler.limit"
	confTunnelBuffererLimit  = "bourne.msg.client.tunnel.bufferer.limit"
	confTunnelSenderLimit    = "bourne.msg.client.tunnel.sender.limit"
	confTunnelVerifyTimeout  = "bourne.msg.client.tunnel.verify.timeout"
	confTunnelSendTimeout    = "bourne.msg.client.tunnel.send.timeout"
	confTunnelRecvTimeout    = "bourne.msg.client.tunnel.recv.timeout"
	confTunnelMaxRetries     = "bourne.msg.client.tunnel.max.retries"
)

const (
	defaultTunnelAssemblerLimit = 1024
	defaultTunnelBuffererLimit  = 1 << 20
	defaultTunnelSenderLimit    = 1 << 18
	defaultTunnelVerifyTimeout  = 5 * time.Second
	defaultTunnelSendTimeout    = 5 * time.Second
	defaultTunnelRecvTimeout    = 5 * time.Second
	defaultTunnelMaxRetries     = 3
)

type Config struct {
	Debug          bool
	AssemblerLimit int
	BuffererLimit  int
	SenderLimit    int
	VerifyTimeout  time.Duration
	RecvTimeout    time.Duration
	SendTimeout    time.Duration
	MaxRetries     int
}

func NewConfig(conf utils.Config) *Config {
	return &Config{
		Debug:          conf.OptionalBool(confTunnelDebug, false),
		AssemblerLimit: conf.OptionalInt(confTunnelAssemblerLimit, defaultTunnelAssemblerLimit),
		BuffererLimit:  conf.OptionalInt(confTunnelBuffererLimit, defaultTunnelBuffererLimit),
		SenderLimit:    conf.OptionalInt(confTunnelSenderLimit, defaultTunnelSenderLimit),
		VerifyTimeout:  conf.OptionalDuration(confTunnelVerifyTimeout, defaultTunnelVerifyTimeout),
		RecvTimeout:    conf.OptionalDuration(confTunnelRecvTimeout, defaultTunnelRecvTimeout),
		SendTimeout:    conf.OptionalDuration(confTunnelSendTimeout, defaultTunnelSendTimeout),
		MaxRetries:     conf.OptionalInt(confTunnelMaxRetries, defaultTunnelMaxRetries)}
}

type TunnelOptions struct {
	Config utils.Config

	// A listening channel
	Listening bool

	// IO
	In  <-chan wire.Packet
	Out chan<- wire.Packet

	// lifecycle handlers
	OnInit  TunnelTransitionHandler
	OnOpen  TunnelTransitionHandler
	OnClose TunnelTransitionHandler
	OnFail  TunnelTransitionHandler
}

type TunnelOptionsHandler func(*TunnelOptions)

type TunnelTransitionHandler func(Tunnel) error

func defaultTunnelOptions() *TunnelOptions {
	return &TunnelOptions{
		Config: utils.NewEmptyConfig(),

		// state handlers
		OnInit:  func(c Tunnel) error { return nil },
		OnOpen:  func(c Tunnel) error { return nil },
		OnClose: func(c Tunnel) error { return nil },
		OnFail:  func(c Tunnel) error { return nil }}
}

// Environment
type Env struct {
	route  wire.Route
	config *Config
}

func (e *Env) Log(format string, vals ...interface{}) {
	if !e.config.Debug {
		return
	}

	log.Println(fmt.Sprintf("[%v] -- ", e.route) + fmt.Sprintf(format, vals...))
}

func (e *Env) recvOrTimeout(in <-chan wire.Packet) (wire.Packet, error) {
	timer := time.NewTimer(e.config.RecvTimeout)

	select {
	case <-timer.C:
		return nil, NewTimeoutError("Timeout")
	case p := <-in:
		return p, nil
	}
}

func (e *Env) sendOrTimeout(out chan<- wire.Packet, p wire.Packet) error {
	timer := time.NewTimer(e.config.SendTimeout)

	select {
	case <-timer.C:
		return NewTimeoutError("Timeout")
	case out <- p:
		return nil
	}
}

type tunnel struct {
	env     *Env
	machine utils.StateMachine

	streamRecv *Stream
	streamSend *Stream
}

func NewTunnel(route wire.Route, opts ...TunnelOptionsHandler) Tunnel {
	// initialize the options.
	defaultOpts := defaultTunnelOptions()
	for _, opt := range opts {
		opt(defaultOpts)
	}
	options := *defaultOpts

	// initialize the environment
	env := &Env{route, NewConfig(options.Config)}

	// create all the channels
	chanBufferer := make(chan []byte)
	chanAssembler := make(chan wire.SegmentMessage)
	chanSendVerify := make(chan wire.NumMessage)
	chanRecvVerify := make(chan wire.NumMessage)
	chanIn := options.In
	chanOut := options.Out

	// create all the workers instances
	streamRecv, bufferer := NewBufferer(env, chanBufferer, chanRecvVerify)
	assembler := NewAssembler(env, chanAssembler, chanBufferer)
	receiver := NewReceiver(env, chanIn, chanAssembler, chanSendVerify)
	streamSend, sender := NewSender(env, chanSendVerify, chanRecvVerify, chanOut)

	builder := utils.BuildStateMachine()
	builder.AddState(TunnelOpeningInit, NewOpenerInit(env, chanIn, chanOut))
	builder.AddState(TunnelOpeningRecv, NewOpenerRecv(env, chanIn, chanOut))
	builder.AddState(TunnelOpeningRecv, bufferer, assembler, receiver, sender)
	builder.AddState(TunnelOpened, bufferer, assembler, receiver, sender)

	var machine utils.StateMachine
	if options.Listening {
		machine = builder.Start(TunnelOpeningInit)
	} else {
		machine = builder.Start(TunnelOpeningRecv)
	}

	return &tunnel{
		env:        env,
		machine:    machine,
		streamRecv: streamRecv,
		streamSend: streamSend}
}

func (t *tunnel) Route() wire.Route {
	return t.env.route
}

func (c *tunnel) Read(p []byte) (n int, err error) {
	return c.streamRecv.Read(p)
}

func (c *tunnel) Write(p []byte) (n int, err error) {
	return c.streamSend.Write(p)
}

func (c *tunnel) Close() error {
	control, err := c.machine.Control()
	if err != nil {
		return err
	}

	control.Transition(TunnelClosingInit)
	return nil
}

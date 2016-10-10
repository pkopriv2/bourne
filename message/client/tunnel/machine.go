package tunnel

import (
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/message/wire"
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
	confTunnelDebug         = "bourne.msg.client.tunnel.debug"
	confTunnelBuffererLimit = "bourne.msg.client.tunnel.bufferer.limit"
	confTunnelSenderLimit   = "bourne.msg.client.tunnel.sender.limit"
	confTunnelVerifyTimeout = "bourne.msg.client.tunnel.verify.timeout"
	confTunnelSendTimeout   = "bourne.msg.client.tunnel.send.timeout"
	confTunnelRecvTimeout   = "bourne.msg.client.tunnel.recv.timeout"
	confTunnelMaxRetries    = "bourne.msg.client.tunnel.max.retries"
)

const (
	defaultTunnelBuffererLimit = 1 << 20
	defaultTunnelSenderLimit   = 1 << 18
	defaultTunnelVerifyTimeout = 5 * time.Second
	defaultTunnelSendTimeout   = 5 * time.Second
	defaultTunnelRecvTimeout   = 5 * time.Second
	defaultTunnelMaxRetries    = 3
)

func recvOrTimeout(ctx common.Context, in <-chan wire.Packet) (wire.Packet, error) {
	timer := time.NewTimer(ctx.Config().OptionalDuration(confTunnelRecvTimeout, defaultTunnelRecvTimeout))

	select {
	case <-timer.C:
		return nil, NewTimeoutError("Timeout")
	case p := <-in:
		return p, nil
	}
}

func sendOrTimeout(ctx common.Context, out chan<- wire.Packet, p wire.Packet) error {
	timer := time.NewTimer(ctx.Config().OptionalDuration(confTunnelSendTimeout, defaultTunnelSendTimeout))

	select {
	case <-timer.C:
		return NewTimeoutError("Timeout")
	case out <- p:
		return nil
	}
}

// complete listing of tunnel channels.
type tunnelChannels struct {
	mainRx <-chan wire.Packet
	mainTx chan<- wire.Packet

	buffererIn   chan []byte
	assemblerIn  chan wire.SegmentMessage
	sendVerifier chan wire.NumMessage
	recvVerifier chan wire.NumMessage
}

func newTunnelChannels(conf utils.Config, mainTx chan<- wire.Packet, mainRx <-chan wire.Packet) *tunnelChannels {
	return &tunnelChannels{
		mainRx:       mainRx,
		mainTx:       mainTx,
		buffererIn:   make(chan []byte),
		assemblerIn:  make(chan wire.SegmentMessage),
		sendVerifier: make(chan wire.NumMessage),
		recvVerifier: make(chan wire.NumMessage)}
}

type MachineSocket struct {
	PacketRx <-chan wire.Packet
	PacketTx chan<- wire.Packet
}

func NewTunnelMachine(ctx common.Context, route wire.Route, streamRx *Stream, streamTx *Stream, socket MachineSocket) utils.StateMachineFactory {
	// initialize all the channels
	channels := newTunnelChannels(ctx.Config(), socket.PacketTx, socket.PacketRx)

	// opening workers
	openerInit := NewOpenerInit(route, ctx, &OpenerSocket{channels.mainRx, channels.mainTx})
	openerRecv := NewOpenerRecv(route, ctx, &OpenerSocket{channels.mainRx, channels.mainTx})

	// closing workers
	closerInit := NewCloserInit(route, ctx, &CloserSocket{channels.mainRx, channels.mainTx})
	closerRecv := NewCloserRecv(route, ctx, &CloserSocket{channels.mainRx, channels.mainTx})

	// opened workers
	sendMain := NewSendMain(route, ctx, streamTx, &SendMainSocket{channels.mainTx, channels.sendVerifier, channels.recvVerifier})
	recvMain := NewRecvMain(ctx, &RecvMainSocket{channels.mainRx, channels.assemblerIn, channels.sendVerifier})
	recvAssembler := NewRecvAssembler(ctx, &AssemblerSocket{channels.assemblerIn, channels.buffererIn, channels.recvVerifier})
	recvBuff := NewRecvBuffer(ctx, &BuffererSocket{channels.buffererIn}, streamRx)

	// build the machine
	builder := utils.BuildStateMachine()
	builder.AddState(TunnelOpeningInit, openerInit)
	builder.AddState(TunnelOpeningRecv, openerRecv)
	builder.AddState(TunnelOpened, sendMain, recvMain, recvAssembler, recvBuff)
	builder.AddState(TunnelClosingInit, closerInit)
	builder.AddState(TunnelClosingRecv, closerRecv)
	return builder
}

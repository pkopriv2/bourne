package tunnel

import (
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/message/core"
	"github.com/pkopriv2/bourne/message/wire"
	"github.com/pkopriv2/bourne/utils"
)

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

type TunnelNetwork struct {
	main         core.DataSocket
	buffererIn   chan []byte
	assemblerIn  chan wire.SegmentMessage
	sendVerifier chan wire.NumMessage
	recvVerifier chan wire.NumMessage
}

func NewTunnelNetwork(conf utils.Config, main core.DataSocket) *TunnelNetwork {
	return &TunnelNetwork{
		main:         main,
		buffererIn:   make(chan []byte),
		assemblerIn:  make(chan wire.SegmentMessage),
		sendVerifier: make(chan wire.NumMessage),
		recvVerifier: make(chan wire.NumMessage)}
}

func (channels *TunnelNetwork) AssemblerSocket() *AssemblerSocket {
	return &AssemblerSocket{channels.assemblerIn, channels.buffererIn, channels.recvVerifier}
}

func (channels *TunnelNetwork) ReceiverSocket() *ReceiverSocket {
	return &ReceiverSocket{channels.main.Rx(), channels.assemblerIn, channels.sendVerifier}
}

func (channels *TunnelNetwork) BuffererSocket() *BuffererSocket {
	return &BuffererSocket{channels.buffererIn}
}

func (channels *TunnelNetwork) SenderSocket() *SenderSocket {
	return &SenderSocket{channels.main.Tx(), channels.sendVerifier, channels.recvVerifier}
}

func (channels *TunnelNetwork) MainSocket() core.DataSocket {
	return channels.main
}

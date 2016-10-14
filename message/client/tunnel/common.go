package tunnel

import (
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/message/core"
	"github.com/pkopriv2/bourne/message/wire"
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
	confTunnelBuffererLimit     = "bourne.message.client.tunnel.bufferer.limit"
	confTunnelSenderLimit       = "bourne.message.client.tunnel.sender.limit"
	confTunnelAssemblerLimit    = "bourne.message.client.tunnel.assembler.limit"
	confTunnelSendVerifierLimit = "bourne.message.client.tunnel.send.verifier.limit"
	confTunnelRecvVerifierLimit = "bourne.message.client.tunnel.recv.verifier.limit"
	confTunnelVerifyTimeout     = "bourne.message.client.tunnel.verify.timeout"
	confTunnelSendTimeout       = "bourne.message.client.tunnel.send.timeout"
	confTunnelRecvTimeout       = "bourne.message.client.tunnel.recv.timeout"
	confTunnelMaxRetries        = "bourne.message.client.tunnel.max.retries"
)

const (
	defaultTunnelBuffererLimit     = 1 << 20
	defaultTunnelSenderLimit       = 1 << 18
	defaultTunnelAssemblerLimit    = 1024
	defaultTunnelSendVerifierLimit = 1024
	defaultTunnelRecvVerifierLimit = 1024
	defaultTunnelVerifyTimeout     = 5 * time.Second
	defaultTunnelSendTimeout       = 5 * time.Second
	defaultTunnelRecvTimeout       = 5 * time.Second
	defaultTunnelMaxRetries        = 3
)

func recvOrTimeout(ctx common.Context, rx <-chan wire.Packet) (wire.Packet, error) {
	timer := time.After(ctx.Config().OptionalDuration(confTunnelRecvTimeout, defaultTunnelRecvTimeout))

	select {
	case <-timer:
		return nil, NewTimeoutError("Timeout")
	case p := <-rx:
		return p, nil
	}
}

func sendOrTimeout(ctx common.Context, tx chan<- wire.Packet, p wire.Packet) error {
	timer := time.After(ctx.Config().OptionalDuration(confTunnelSendTimeout, defaultTunnelSendTimeout))

	select {
	case <-timer:
		return NewTimeoutError("Timeout")
	case tx <- p:
		return nil
	}
}

// complete listing of tunnel channels.

type TunnelNetwork interface {
	MainSocket() core.DataSocket
	ReceiverSocket() *ReceiverSocket
	AssemblerSocket() *AssemblerSocket
	BuffererSocket() *BuffererSocket
	SenderSocket() *SenderSocket
	VerifierSocket() *VerifierSocket
}

type tunnelNetwork struct {
	main         core.DataSocket
	buffererIn   chan []byte
	assemblerIn  chan wire.SegmentMessage
	sendVerifier chan wire.NumMessage
	recvVerifier chan wire.NumMessage
}

func NewTunnelNetwork(conf common.Config, main core.DataSocket) TunnelNetwork {
	return &tunnelNetwork{
		main:         main,
		buffererIn:   make(chan []byte, conf.OptionalInt(confTunnelBuffererLimit, defaultTunnelBuffererLimit)),
		assemblerIn:  make(chan wire.SegmentMessage, conf.OptionalInt(confTunnelAssemblerLimit, defaultTunnelAssemblerLimit)),
		sendVerifier: make(chan wire.NumMessage, conf.OptionalInt(confTunnelSendVerifierLimit, defaultTunnelSendVerifierLimit)),
		recvVerifier: make(chan wire.NumMessage, conf.OptionalInt(confTunnelRecvVerifierLimit, defaultTunnelRecvVerifierLimit))}
}

func (channels *tunnelNetwork) AssemblerSocket() *AssemblerSocket {
	return &AssemblerSocket{channels.assemblerIn, channels.buffererIn, channels.recvVerifier}
}

func (channels *tunnelNetwork) ReceiverSocket() *ReceiverSocket {
	return &ReceiverSocket{channels.main.Rx(), channels.assemblerIn, channels.sendVerifier}
}

func (channels *tunnelNetwork) BuffererSocket() *BuffererSocket {
	return &BuffererSocket{channels.buffererIn}
}

func (channels *tunnelNetwork) SenderSocket() *SenderSocket {
	return &SenderSocket{channels.main.Tx(), channels.sendVerifier}
}

func (channels *tunnelNetwork) VerifierSocket() *VerifierSocket {
	return &VerifierSocket{channels.main.Tx(), channels.recvVerifier}
}

func (channels *tunnelNetwork) MainSocket() core.DataSocket {
	return channels.main
}

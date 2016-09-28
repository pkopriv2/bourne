package tunnel

import (
	"fmt"
	"log"
	"time"

	"github.com/pkopriv2/bourne/msg/wire"
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
	confTunnelDebug        = "bourne.msg.tunnel.debug"
	confTunnelRecvInSize   = "bourne.msg.tunnel.recv.in.size"
	confTunnelRecvLogSize  = "bourne.msg.tunnel.recv.log.size"
	confTunnelSendLogSize  = "bourne.msg.tunnel.send.log.size"
	confTunnelSendWait     = "bourne.msg.tunnel.send.wait"
	confTunnelRecvWait     = "bourne.msg.tunnel.recv.wait"
	confTunnelAckTimeout   = "bourne.msg.tunnel.ack.timeout"
	confTunnelCloseTimeout = "bourne.msg.tunnel.close.size"
	confTunnelMaxRetries   = "bourne.msg.tunnel.max.retries"
)

const (
	defaultTunnelRecvInSize   = 1024
	defaultTunnelRecvLogSize  = 1 << 20 // 1024K
	defaultTunnelSendLogSize  = 1 << 18 // 256K
	defaultTunnelSendWait     = 100 * time.Millisecond
	defaultTunnelRecvWait     = 20 * time.Millisecond
	defaultTunnelAckTimeout   = 5 * time.Second
	defaultTunnelCloseTimeout = 10 * time.Second
	defaultTunnelMaxRetries   = 3
)

type config struct {
	debug bool

	AssemblerMax int
	BuffererMax int
	SenderMax int

	RecvTimeout time.Duration
	SendTimeout time.Duration
	ackTimeout time.Duration
	maxRetries int
}

// Environment
type Env struct {
	route wire.Route
	conf  *config
}

// helper functions
func (c *Env) Log(format string, vals ...interface{}) {
	if !c.conf.debug {
		return
	}

	log.Println(fmt.Sprintf("[%v] -- ", c.route) + fmt.Sprintf(format, vals...))
}

func (c *Env) recvOrTimeout(in <-chan wire.Packet) (wire.Packet, error) {
	timer := time.NewTimer(c.conf.RecvTimeout)

	select {
	case <-timer.C:
		return nil, NewTimeoutError("Timeout")
	case p := <-in:
		return p, nil
	}
}

func (c *Env) sendOrTimeout(out chan<- wire.Packet, p wire.Packet) error {
	timer := time.NewTimer(c.conf.SendTimeout)

	select {
	case <-timer.C:
		return NewTimeoutError("Timeout")
	case out<-p:
		return nil
	}
}

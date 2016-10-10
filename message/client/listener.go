package client

import (
	"fmt"
	"sync"

	"github.com/pkopriv2/bourne/message/wire"
)

// Listeners await channel requests and spawn new channels.
// Consumers should take care to hand the received channel
// to a separate thread as quickly as possible, as this
// blocks additional channel requests.
//
// Closing a listener does NOT affect any channels that have
// been spawned.
//
// *Implementations must be thread-safe*
//
type Listener interface {
	wire.Routable

	Accept() (Tunnel, error)
}

// A listener is a simple channel awaiting new channel requests.
//
// *This object is thread safe.*
//
type listener struct {
	lock   sync.RWMutex
	socket ListenerSocket
	closed bool
}

func newListener(socket ListenerSocket) Listener {
	return &listener{socket: socket}
}

func (l *listener) Route() wire.Route {
	return l.socket.Route()
}

func (l *listener) Accept() (Tunnel, error) {
	var p wire.Packet
	select {
	case <-l.socket.Closed():
		return nil, fmt.Errorf("Socket closed")
	case <-l.socket.Failed():
		return nil, l.socket.Failure()
	case p = <-l.socket.Rx():
	}

	tunnelSocket, err := l.socket.NewTunnel(p.Route().Src())
	if err != nil {
		return nil, err
	}

	tunnelSocket.Tx() <- p
	return nil, nil
}

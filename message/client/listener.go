package client

import (
	"io"

	"github.com/pkopriv2/bourne/errors"
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
var (
	NewListenerClosedError = errors.NewWrappedError("LISTENER:CLOSED[reason=%v]")
)

type Listener interface {
	io.Closer

	Route() wire.Route
	Accept() (Tunnel, error)
}

// A listener is a simple channel awaiting new channel requests.
//
// *This object is thread safe.*
//
type listener struct {
	socket ListenerSocket
	close  chan struct{}
	closed chan struct{}
	result error
}

func newListener(socket ListenerSocket) Listener {
	return &listener{socket: socket}
}

func (l *listener) Route() wire.Route {
	return l.socket.Route()
}

func (t *listener) Close() error {
	select {
	case <-t.closed:
		return NewListenerClosedError(t.result)
	case t.close <- struct{}{}:
	}

	<-t.closed
	return nil
}

func controlListener(l *listener) {
	defer l.socket.Done()

	select {
	case <-l.close:
	case <-l.socket.Closed():
	case <-l.socket.Failed():
		l.result = l.socket.Failure()
	}

	close(l.closed)
}

func (l *listener) Accept() (Tunnel, error) {
	var p wire.Packet
	select {
	case <-l.closed:
		return nil, NewListenerClosedError(l.result)
	case p = <-l.socket.Rx():
	}

	tunnelSocket, err := l.socket.NewTunnel(p.Route().Src())
	if err != nil {
		return nil, err
	}

	tunnel := NewTunnel(tunnelSocket)
	select {
	case <-tunnelSocket.Closed():
		return nil, NewListenerClosedError(tunnelSocket.Failure())
	case <-tunnelSocket.Failed():
		return nil, NewListenerClosedError(tunnelSocket.Failure())
	case tunnelSocket.Tx() <- p:
		return tunnel, nil
	}
}

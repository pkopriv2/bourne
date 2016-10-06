package client

import (
	"sync"

	"github.com/pkopriv2/bourne/msg/client/tunnel"
	"github.com/pkopriv2/bourne/msg/wire"
)

const (
	confListenerRecvInSize = "bourne.msg.listener.recv.in.size"
)

const (
	defaultListenerRecvInSize = 1024
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

	// Accepts a channel request.  Blocks until one
	// is available.  Returns a non-nil error if the
	// listener has been closed.
	//
	Accept() (tunnel.Tunnel, error)
}

// A listener is a simple channel awaiting new channel requests.
//
// *This object is thread safe.*
//
type listener struct {
	lock   sync.RWMutex
	route  wire.Route
	in     chan wire.Packet
	closed bool
}

// Creates and returns a new listening channel.  This has the side effect of adding the
// channel to the channel router, which means that it immediately available to have
// packets routed to it.
//
func newListener(subscription ListenerSubscription) (*listener, error) {

	// create the channel
	listener := &listener{
		route:   route,
		options: options,
		in:      make(chan wire.Packet, options.Config.OptionalInt(confListenerRecvInSize, defaultListenerRecvInSize))}

	// finally, return control to the caller
	return listener, nil
}

func (l *listener) Route() wire.Route {
	return l.route
}

func (l *listener) Accept() (tunnel.Tunnel, error) {
	packet, ok := <-l.in
	if !ok {
		return nil, ErrChannelClosed
	}

return l.tryAccept(packet)
}

// split out for a more granular locking strategy
func (l *listener) tryAccept(p wire.Packet) (Channel, error) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if l.closed {
		return nil, ErrChannelClosed
	}

	channel := newChannel(p.Route().Reverse(), true, l.options.OnSpawn, func(opts *ChannelOptions) {
		opts.Config = l.options.Config
	})
	if err := channel.send(p); err != nil {
		return nil, ErrChannelClosed
	}

	return channel, nil
}

// Sends a packet to the channel stream.
//
func (l *listener) send(p wire.Packet) error {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if l.closed {
		return ErrChannelClosed
	}

	l.in <- p
	return nil
}
//
// // Closes the channel.  Returns an error the if the
// // channel is already closed.
// //
// func (l *listener) Close() error {
// l.lock.Lock()
// defer l.lock.Unlock()
// if l.closed {
// return ErrChannelClosed
// }
//
// // close the buffer
// close(l.in)
//
// // call the on close handler
// l.options.OnClose(l)
//
// // finally, mark it closed
// l.closed = true
// return nil
// }

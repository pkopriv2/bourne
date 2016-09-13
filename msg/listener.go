package msg

import (
	"sync"
)

const (
	// defaultListenerMaxId   = 255
	// defaultListenerMinId   = 0
	defaultListenerBufSize = 1024
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
	Routable

	// Accepts a channel request.  Blocks until one
	// is available.  Returns a non-nil error if the
	// listener has been closed.
	//
	Accept() (Channel, error)
}

// Function to be called when configuring a listener.
type ListenerOptionsHandler func(*ListenerOptions)

// Function to be called when state transitions occur.
type ListenerTransitionHandler func(Listener) error

// listener options struct
type ListenerOptions struct {

	// Whether or not to enable debug logging.
	Debug bool

	// Size of the input buffe
	ListenerBufSize int

	// called when the listener has been close
	OnClose ListenerTransitionHandler

	// called when a new channel is spawned.
	OnSpawn ChannelOptionsHandler
}

// Returns the default options.
func defaultListenerOptions() *ListenerOptions {
	return &ListenerOptions{ListenerBufSize: defaultListenerBufSize}
}

// A listener is a simple channel awaiting new channel requests.
//
// *This object is thread safe.*
//
type listener struct {
	// the listener lock (used to synchronize state transitions)
	lock sync.RWMutex

	// the session address (will have a nil remote address)
	session Session

	// the buffered "in" channel (owned by this channel)
	in chan *Packet

	// options functions (called for each spawned channel)
	options ListenerOptions

	// a flag indicating that the channel is closed (updates/reads must be synchronized)
	closed bool
}

// Creates and returns a new listening channel.  This has the side effect of adding the
// channel to the channel router, which means that it immediately available to have
// packets routed to it.
//
func newListener(session Session, opts ...ListenerOptionsHandler) (*listener, error) {

	// initialize the options.
	defaultOpts := defaultListenerOptions()
	for _, opt := range opts {
		opt(defaultOpts)
	}

	// defensively copy the options (this is to eliminate any reference to the options that the consumer may have)
	options := *defaultOpts

	// create the channel
	listener := &listener{
		session: session,
		options: options,
		in:      make(chan *Packet, options.ListenerBufSize)}

	// finally, return control to the caller
	return listener, nil
}

func (l *listener) Session() Session {
	return l.session
}

func (l *listener) Accept() (Channel, error) {
	packet, ok := <-l.in
	if !ok {
		return nil, ErrChannelClosed
	}

	return l.tryAccept(packet)
}

// split out for a more granular locking strategy
func (l *listener) tryAccept(p *Packet) (Channel, error) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if l.closed {
		return nil, ErrChannelClosed
	}

	lAddr := NewEndPoint(p.srcEntityId, p.srcChannelId)
	rAddr := NewEndPoint(p.dstEntityId, p.dstChannelId)

	channel := newChannel(lAddr, rAddr, true, l.options.OnSpawn)
	if err := channel.send(p); err != nil {
		return nil, ErrChannelClosed
	}

	return channel, nil
}

// Sends a packet to the channel stream.
//
func (l *listener) send(p *Packet) error {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if l.closed {
		return ErrChannelClosed
	}

	l.in <- p
	return nil
}

// Closes the channel.  Returns an error the if the
// channel is already closed.
//
func (l *listener) Close() error {
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.closed {
		return ErrChannelClosed
	}

	// close the buffer
	close(l.in)

	// call the on close handler
	l.options.OnClose(l)

	// finally, mark it closed
	l.closed = true
	return nil
}

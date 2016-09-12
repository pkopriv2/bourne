package msg

import (
	"errors"
	"sync"
)

var (
	ErrListenerInvalidId = errors.New("LISTENER:INVALD_ID")
)

const (
	ListenerMaxId   = 255
	ListenerMinId   = 0
	ListenerBufSize = 1024
)

// A listener is a simple channel awaiting new channel requests.
//
// *This object is thread safe.*
//
type listener struct {

	// the session address (will have a nil remote address)
	session Session

	// the channel cache (shared amongst all channels)
	cache *ChannelCache

	// the buffered "in" channel (owned by this channel)
	in chan Packet

	// the buffered "out" channel (shared amongst all channels)
	out chan Packet

	// options functions (called on each spawned channel)
	config ChannelConfig

	// the lock on this channel
	lock sync.RWMutex

	// a flag indicating that the channel is closed (updates/reads must be synchronized)
	closed bool
}

// Creates and returns a new listening channel.  This has the side effect of adding the
// channel to the channel cache, which means that it immediately available to have
// packets routed to it.
//
func newListener(local Address, cache *ChannelCache, out chan Packet, config ChannelConfig) (*listener, error) {

	// buffered input chan
	in := make(chan Packet, ListenerBufSize)

	// create the channel
	listener := &listener{
		session: NewListenerSession(local),
		cache:   cache,
		out:     out,
		config:  config,
		in:      in}

	// add it to the channel pool (i.e. make it available for routing)
	if err := cache.Add(listener); err != nil {
		return nil, err
	}

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

	return l.tryAccept(&packet)
}

// split out for a more granular locking strategy
func (l *listener) tryAccept(p *Packet) (Channel, error) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if l.closed {
		return nil, ErrChannelClosed
	}

	lAddr := NewAddress(p.srcEntityId, p.srcChannelId)
	rAddr := NewAddress(p.dstEntityId, p.dstChannelId)

	channel := newChannel(lAddr, rAddr, true, func(opts *ChannelOptions) {
		l.config(opts)

		// these event handlers must always win!
		opts.OnData = func(p *Packet) error {
			l.out <- *p
			return nil
		}

		opts.OnClose = func(c Channel) error {
			defer l.cache.Remove(c.Session())
			return nil
		}

		opts.OnFailure = func(c Channel) error {
			defer l.cache.Remove(c.Session())
			return nil
		}
	})

	if err := l.cache.Add(channel); err != nil {
		return nil, err
	}

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

	l.in <- *p
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

	// stop routing to this channel.
	l.cache.Remove(l.session)

	// close the buffer
	close(l.in)

	// finally, mark it closed
	l.closed = true
	return nil
}

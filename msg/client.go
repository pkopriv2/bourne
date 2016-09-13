package msg

import (
	"errors"
	"io"
)

// Error types
var (
	ErrChannelClosed   = errors.New("CHAN:CLOSED")
	ErrChannelFailure  = errors.New("CHAN:FAILURE")
	ErrChannelResponse = errors.New("CHAN:RESPONSE")
	ErrChannelTimeout  = errors.New("CHAN:TIMEOUT")
	ErrChannelExists   = errors.New("CHAN:EXISTS")
	ErrChannelUnknown  = errors.New("CHAN:UNKNONW")
)

// Basic channel identity.   This identifies one side of a "session".
//
type EndPoint interface {
	EntityId() uint32
	ChannelId() uint16
}

type endpoint struct {
	entityId  uint32
	channelId uint16
}

func NewEndPoint(entityId uint32, channelId uint16) EndPoint {
	return endpoint{entityId, channelId}
}

func (c endpoint) EntityId() uint32 {
	return c.entityId
}

func (c endpoint) ChannelId() uint16 {
	return c.channelId
}

// A session address is the tuple necessary to uniquely identify
// a channel within a shared stream.  All packet routing is done
// based on complete session address
//
type Session interface {
	Local() EndPoint
	Remote() EndPoint // nil for listeners
}

type session struct {
	local  EndPoint
	remote EndPoint // nil for listeners.
}

func (s session) Local() EndPoint {
	return s.local
}

func (s session) Remote() EndPoint {
	return s.remote
}

func NewListenerSession(local EndPoint) Session {
	return session{local, nil}
}

func NewChannelSession(local EndPoint, remote EndPoint) Session {
	return session{local, remote}
}

// Accepts packets as input.
//
// *Implementations must be thread-safe*
//
type Routable interface {
	io.Closer

	// Returns the complete session address of this channel.
	Session() Session

	// Sends a packet the processors input channel.  Each implementation
	// should attempt to implement this in a NON-BLOCKING
	// fashion. However, may block if necessary.
	//
	send(p *Packet) error
}

// A channel represents one side of an active sessin.
//
// *Implementations must be thread-safe*
//
type Channel interface {
	Routable
	io.Reader
	io.Writer
}

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


// The primary client interface.
type Client interface {
	io.Closer

	// Each client is identified by a primary entity identifier.
	EntityId() uint32

	// Connects to the remote endpoint.
	Connect(remote EndPoint) (Channel, error)

	// Begins listening on
	Listen(channelId uint32) (Listener, error)
}

// func ServerHandler
//
// func Serve(l Listener)

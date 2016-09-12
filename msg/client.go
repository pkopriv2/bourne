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
type Address interface {
	EntityId() uint32
	ChannelId() uint16
}

type address struct {
	entityId  uint32
	channelId uint16
}

func NewAddress(entityId uint32, channelId uint16) Address {
	return address{entityId, channelId}
}

func (c address) EntityId() uint32 {
	return c.entityId
}

func (c address) ChannelId() uint16 {
	return c.channelId
}

// A session address is the tuple necessary to uniquely identify
// a channel within a shared stream.  All packet routing is done
// based on complete session address
//
type Session interface {
	Local() Address
	Remote() Address // nil for listeners
}

type session struct {
	local  Address
	remote Address // nil for listeners.
}

func (s session) Local() Address {
	return s.local
}

func (s session) Remote() Address {
	return s.remote
}

func NewListenerSession(local Address) Session {
	return session{local, nil}
}

func NewChannelSession(local Address, remote Address) Session {
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

//
// type Client interface {
//
// }

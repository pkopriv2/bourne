package msg

import (
	"errors"
	"io"
)

// Error types
var CHANNEL_EXISTS_ERROR = errors.New("CHAN:EXISTS")
var CHANNEL_UNKNOWN_ERROR = errors.New("CHAN:UNKNONW")
var CHANNEL_CLOSED_ERROR = errors.New("CHAN:CLOSED")
var CHANNEL_REFUSED_ERROR = errors.New("CHAN:REFUSED")

// A channel address is the tuple necessary to uniquely identify
// a channel within a shared stream.
//
type ChannelAddress struct {
	entityId  uint32
	channelId uint16
}

// A channel represents one side of an active conversation between two
// entities.
//
// *Implementations must be thread-safe*
//
type Channel interface {
	io.Closer
	io.Reader
	io.Writer

	// Returns the local address of the channel.
	// never nil or zero value.
	//
	LocalAddr() ChannelAddress

	// Returns the local address of the channel.
	// never nil or zero value.
	//
	RemoteAddr() ChannelAddress
}


// Listeners await channel requests and spawn new channels.
// Consumers should take care to hand the received channel
// to a separate thread as quickly as possible, as this
// blocks additional channel requests.
//
// Closing a listener does NOT affect any channels that have
// been spawned.
//
// Implementations are *thread-safe*, therefore the same
// listener can be shared amongst many threads.
//
type Listener interface {
	io.Closer

	// Accepts a channel request.  Blocks until one
	// is available.  Returns a non-nil error if the
	// listener has been closed.
	//
	Accept() (*Channel, error)
}

package net

import (
	"errors"
	"io"
)

var ConnectionClosedError = errors.New("CONN:CLOSED")
var ListenerClosedError = errors.New("CONN:LISTENER:CLOSED")

// A connection is a full-duplex streaming abstraction.
//
// Implementations are expected to be thread-safe, with
// respect to concurrent reads and writes.
type Connection interface {
	io.Reader
	io.Writer
	io.Closer
}

// Connection factories are used to create the underlying streams.  In
// the event of failure, this allows streams to be "recreated", without
// leaking how the streams are generated.  The intent is to create a
// highly resilient multiplexer.
//
// Consumers should not retain any references to the created connection.
type ConnectionFactory interface {
	Conn() (Connection, error)
}


// A simple listener abstraction.  This will be the basis of
type Listener interface {
	io.Closer
	Conn() (Connection, error)
	Accept() (Connection, error)
}

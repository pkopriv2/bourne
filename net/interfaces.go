package net

import (
	"errors"
	"fmt"
	"io"

	"github.com/pkopriv2/bourne/enc"
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

	Factory() ConnectionFactory
}

// A simple listener abstraction.  This will be the basis of
// establishing network services
type Listener interface {
	io.Closer
	Conn() (Connection, error)
	Accept() (Connection, error)
}

// Connection factories are used to create the underlying streams.  In
// the event of failure, this allows streams to be "recreated", without
// leaking how the streams are generated.  The intent is to create a
// highly resilient multiplexer.
//
// Consumers should not retain any references to the created connection.
type ConnectionFactory interface {
	enc.Writable
	Conn() (Connection, error)
}

// Returns a connection factory from the raw message data, the input
// of which is expected to be in the format produced via ConnectionFactory#Write()
func ReadConnectionFactory(m enc.Reader) (ConnectionFactory, error) {
	var typ string
	if err := m.Read("type", &typ); err != nil {
		return nil, err
	}

	switch typ {
	default:
		return nil, fmt.Errorf("Cannot parse connection factory [%v]  Unknown connection 'type' field.", typ)
	case "tcp":
		return ReadTcpConnectionFactory(m)
	}
}

func NewAddr(host string, port int) string {
	return fmt.Sprintf("%v:%v", host, port)
}

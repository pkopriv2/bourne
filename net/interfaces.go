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
	// encoding.Encodable
	Conn() (Connection, error)
}

// Returns a connection factory from the raw serlialized data, the input
// of which is expected to be in the format produced via ConnectionFactory#Serialize()
func ParseConnectionFactory(data []byte) (ConnectionFactory, error) {
	return nil, nil
	// data, ok := raw.(ConnectionFactoryData)
	// if ! ok {
	// return nil, fmt.Errorf("Cannot parse connection factory [%v].  Wrong data type", data)
	// }
	//
	// switch data.Type {
	// default:
	// return nil, fmt.Errorf("Cannot parse connection factory [%v]  Missing 'type' field.", data)
	// case "tcp":
	// return NewTCPConnectionFactory(data.Addr), nil
	// }
}

type SerialiedConnectionFactory struct {
	Type string
	Addr string
}

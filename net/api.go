package net

import (
	"errors"
	"io"
	"net"
)

var (
	ClosedError = errors.New("Net:Closed")
)

// A connection is a full-duplex streaming abstraction.
//
// Implementations are expected to be thread-safe, with
// respect to concurrent reads and writes.
type Connection interface {
	io.Reader
	io.Writer
	io.Closer

	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}

// A simple listener abstraction.  This will be the basis of
// establishing network services
type Listener interface {
	io.Closer
	Addr() net.Addr
	Conn() (Connection, error)
	Accept() (Connection, error)
}

func NewAddr(host string, port string) string {
	return net.JoinHostPort(host, port)
}

func SplitAddr(addr string) (string, string, error) {
	return net.SplitHostPort(addr)
}

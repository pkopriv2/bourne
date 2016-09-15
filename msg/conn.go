package msg

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

var (
	ErrConnectionClosed  = errors.New("CONN:ERR:CLOSED")
	ErrConnectionFailure = errors.New("CONN:ERR:FAILURE")
	ErrConnectionTimeout = errors.New("CONN:ERR:TIMEOUT")
)

// A connection is a full-duplex streaming abstraction.
//
// Implementations are expected to be thread-safe, with
// respect to concurrent reads and writes.
type Connection interface {
	io.Reader
	io.Writer
	io.Closer
}

// In order to track the connection attempts and retain history of
// errors, as we're attempting to reconnect, we'll add errors
// that we encounter.  These will ultimately be relayed to the
// consuming code.
type ConnectionErrors []error

func (c ConnectionErrors) Error() string {
	var buffer bytes.Buffer
	buffer.WriteString("Connection Errors: ")

	for i, err := range c {
		buffer.WriteString(fmt.Sprintf("\n\t[%v](%v)", i, err))
	}

	return buffer.String()
}

// Connection factories are used to create the underlying streams.  In
// the event of failure, this allows streams to be "recreated", without
// leaking how the streams are generated.  The intent is to create a
// highly resilient multiplexer.
//
// Consumers should not retain any references to the created connection.
type ConnectionFactory func() (Connection, error)

// A connector is a thread-safe, highly resilient connection.
//
// Upon experiencing an read/write failure, this object will attempt to
// reset its internal connection and try again.  It will do this up to
// the specified number of retries.
//
// This class is never truly dead until close is called on it, in which
// case all read/writes will return ErrConnectionClosed.  This will always
// attempt to reconnect on each read/write.
type Connector struct {
	Connection

	lock    sync.RWMutex
	factory ConnectionFactory
	conn    Connection
	closed  bool
	retries int
	timeout time.Duration
}

func NewConnector(factory ConnectionFactory, retries int, timeout time.Duration) *Connector {
	return &Connector{
		factory: factory,
		retries: retries,
		timeout: timeout}
}

func (c *Connector) get(reset bool) (Connection, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.closed {
		return nil, ErrConnectionClosed
	}

	if c.conn != nil && !reset {
		return c.conn, nil
	}

	var err error
	c.conn, err = connect(c.factory, c.timeout)
	return c.conn, err
}

func (c *Connector) Read(p []byte) (int, error) {
	var errors ConnectionErrors
	var conn Connection
	var err error
	var n int

	conn, err = c.get(false)
	for i := 0; i < c.retries; i++ {
		if conn != nil {
			if n, err = conn.Read(p); err != nil {
				return n, err
			}
		}

		errors = append(errors, err)
		conn, err = c.get(true)
	}

	return 0, errors

}

func (c *Connector) Write(p []byte) (int, error) {
	var errors ConnectionErrors
	var conn Connection
	var err error
	var n int

	conn, err = c.get(false)
	for i := 0; i < c.retries; i++ {
		if conn != nil {
			if n, err = conn.Write(p); err != nil {
				return n, err
			}
		}

		errors = append(errors, err)
		conn, err = c.get(true)
	}

	return 0, errors
}

func (c *Connector) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.closed {
		return ErrConnectionClosed
	}

	c.closed = true
	c.conn = nil
	return nil
}

func connect(factory ConnectionFactory, timeout time.Duration) (Connection, error) {
	type attempt struct {
		conn Connection
		err  error
	}

	// this will necessarily leak a go routine for each timeout.
	out := make(chan attempt)
	go func() {
		conn, err := factory()
		out <- attempt{conn, err}
	}()

	timer := time.NewTimer(timeout)
	select {
	case <-timer.C:
		return nil, ErrConnectionTimeout
	case attempt := <-out:
		return attempt.conn, attempt.err
	}
}

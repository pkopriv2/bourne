package net

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pkopriv2/bourne/common"
)

const (
	confConnectionRetries = "bourne.msg.connection.retries"
	confConnectionTimeout = "bourne.msg.connection.timeout"
)

const (
	defaultConnectionRetries = 3
	defaultConnectionTimeout = 30 * time.Second
)

var (
	ErrConnectionClosed = errors.New("CONN:ERR:CLOSED")
)

type ConnectionTimeoutError struct {
	Duration time.Duration
}

func (c ConnectionTimeoutError) Error() string {
	return fmt.Sprintf("CONN:ERR:TIMEOUT(%v)", c.Duration)
}

type ConnectionError struct {
	attempts []error
}

func (c ConnectionError) Error() string {
	buffer := new(bytes.Buffer)
	buffer.WriteString("Errors: ")

	for i, err := range c.attempts {
		buffer.WriteString(fmt.Sprintf("\n\t[%v](%v)", i, err))
	}

	return buffer.String()
}

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
type ConnectionFactory func() (Connection, error)

// A connector is a thread-safe, highly resilient connection.
//
// Upon experiencing an read/write failure, this object will attempt to
// reset its internal connection and try again.  It will do this up to
// the specified number of retries.
//
// This class is never truly dead until close is called on it, in which
// case all read/writes will return ErrClosed.  This will always
// attempt to reconnect on each read/write.
type connection struct {
	Connection

	lock    sync.RWMutex
	factory ConnectionFactory
	conn    Connection
	closed  bool
	retries int
	timeout time.Duration
}

func NewConnector(factory ConnectionFactory, config common.Config) *connection {
	retries := config.OptionalInt(confConnectionRetries, defaultConnectionRetries)
	timeout := config.OptionalDuration(confConnectionTimeout, defaultConnectionTimeout)

	return &connection{
		factory: factory,
		retries: retries,
		timeout: timeout}
}

func (c *connection) get(reset bool) (Connection, error) {
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

func (c *connection) Read(p []byte) (int, error) {
	var errors []error = make([]error, 1, c.retries)
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

	return 0, ConnectionError{errors}
}

func (c *connection) Write(p []byte) (int, error) {
	var errors []error = make([]error, 1, c.retries)
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

	return 0, ConnectionError{errors}
}

func (c *connection) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.closed {
		return ErrConnectionClosed
	}

	var err error

	c.closed = true
	if c.conn != nil {
		err = c.conn.Close()
	}
	c.conn = nil
	return err
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
		return nil, ConnectionTimeoutError{timeout}
	case attempt := <-out:
		return attempt.conn, attempt.err
	}
}

package common

import (
	"io"

	"github.com/pkopriv2/bourne/concurrent"
)

type Control interface {
	io.Closer
	Fail(error)
	Closed() <-chan struct{}
	IsClosed() bool
	Failure() error
	OnClose(func(error))
	Sub() Control
}

type control struct {
	closes  concurrent.List
	closed  chan struct{}
	closer  chan struct{}
	failure error
}

func NewControl(parent *control) *control {
	l := &control{
		closes: concurrent.NewList(8),
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1),
	}

	if parent != nil {
		go func() {
			select {
			case <-parent.Closed():
				l.Fail(parent.Failure())
				return
			case <-l.closed:
				return
			}
		}()
	}

	return l
}

func (c *control) Fail(cause error) {
	select {
	case <-c.closed:
		return
	case c.closer <- struct{}{}:
	}

	c.failure = cause
	close(c.closed)

	for _, fn := range c.closes.All() {
		fn.(func(error))(cause)
	}
}

func (c *control) Close() error {
	c.Fail(nil)
	return c.Failure()
}

func (c *control) Closed() <-chan struct{} {
	return c.closed
}

func (c *control) IsClosed() bool {
	select {
	default:
		return false
	case <-c.closed:
		return true
	}
}

func (c *control) Failure() error {
	<-c.closed
	return c.failure
}

func (c *control) OnClose(fn func(error)) {
	c.closes.Append(fn)
}

func (c *control) Sub() Control {
	return NewControl(c)
}

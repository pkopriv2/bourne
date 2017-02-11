package common

import (
	"io"

	"github.com/pkopriv2/bourne/concurrent"
)

func IsCanceled(cancel <-chan struct{}) bool {
	return IsClosed(cancel)
}

func IsClosed(ch <-chan struct{}) bool {
	select {
	default:
		return false
	case <-ch:
		return true
	}
}

type Control interface {
	io.Closer
	Fail(error)
	Closed() <-chan struct{}
	IsClosed() bool
	Failure() error
	Defer(func(error))
	Sub() Control
}

type control struct {
	closes  concurrent.List
	closed  chan struct{}
	closer  chan struct{}
	failure error
}

func NewControl(parent Control) *control {
	l := &control{
		closes: concurrent.NewList(8),
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1),
	}

	if parent != nil {
		parent.Defer(func(e error) {
			l.Fail(parent.Failure())
		})
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

func (c *control) Defer(fn func(error)) {
	c.closes.Prepend(fn)
}

func (c *control) Sub() Control {
	return NewControl(c)
}

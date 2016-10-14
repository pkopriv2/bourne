package circuit

import (
	"errors"
	"time"

	"github.com/pkopriv2/bourne/concurrent"
)

var ControllerClosedError = errors.New("ERR:CONTROLLER:CLOSED")
var ControllerShutdownError = errors.New("ERR:CONTROLLER:SHUTDOWN")

type Controller interface {
	Close() error
	Fail(error)
	NewControlSocket() (ControlSocket, error)
}

type ControlSocket interface {
	Closed() <-chan struct{}
	Failed() <-chan struct{}
	Failure() error
	Done()
}

type controller struct {
	closed  chan struct{}
	failed  chan struct{}
	close   chan struct{}
	fail    chan error
	failure concurrent.Val
	wait    concurrent.Wait
}

func NewController() Controller {
	ret := &controller{
		closed:  make(chan struct{}),
		failed:  make(chan struct{}),
		close:   make(chan struct{}, 1),
		fail:    make(chan error, 1),
		failure: concurrent.NewVal(error(nil)),
		wait:    concurrent.NewWait(),
	}

	ret.wait.Inc()
	go control(ret)
	return ret
}

func (c *controller) Wait() <-chan struct{} {
	return c.wait.Wait()
}

func (c *controller) WaitForShutdown() error {
	timer := time.After(5 * time.Second)
	select {
	case <-c.Wait():
		return nil
	case <-timer:
		return ControllerShutdownError
	}
}

func (c *controller) Close() error {
	select {
	case <-c.failed:
		return ControllerClosedError
	case <-c.closed:
		return ControllerClosedError
	case c.close <- struct{}{}:
	}

	return c.WaitForShutdown()
}

func (c *controller) Fail(e error) {

	select {
	case <-c.failed:
		return
	case <-c.closed:
		return
	case c.fail <- e:
	}

	c.WaitForShutdown()
}

func (c *controller) Failure() error {
	err, ok := c.failure.Get().(error)
	if !ok {
		return nil
	}

	return err
}

func control(c *controller) {
	defer c.wait.Dec()

	select {
	case e := <-c.fail:
		c.failure.Set(e)
		close(c.failed)
	case <-c.close:
		close(c.closed)
	}
}

func (c *controller) NewControlSocket() (ControlSocket, error) {
	select {
	default:
	case <-c.failed:
		return nil, ControllerClosedError
	case <-c.closed:
		return nil, ControllerClosedError
	}

	c.wait.Inc()
	return &controlSocket{c}, nil
}

type controlSocket struct {
	parent *controller
}

func (c *controlSocket) Closed() <-chan struct{} {
	return c.parent.closed
}

func (c *controlSocket) Failed() <-chan struct{} {
	return c.parent.failed
}

func (c *controlSocket) Failure() error {
	return c.parent.Failure()
}

func (c *controlSocket) Done() {
	c.parent.wait.Dec()
}

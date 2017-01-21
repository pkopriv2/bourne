package common

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/concurrent"
)

type Env interface {
	Closed() <-chan struct{}
	OnClose(func())
	Data() concurrent.Map
}

type env struct {
	data   concurrent.Map
	closes concurrent.List
	closed chan struct{}
	closer chan struct{}
}

func NewEnv() *env {
	return &env{
		data:   concurrent.NewMap(),
		closes: concurrent.NewList(8),
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1),
	}
}

func (c *env) Close() error {
	select {
	case <-c.closed:
		return errors.Errorf("Already closed")
	case c.closer <- struct{}{}:
	}

	for _, fn := range c.closes.All() {
		fn.(func())()
	}

	close(c.closed)
	return nil
}

func (e *env) Closed() <-chan struct{} {
	return e.closed
}

func (c *env) OnClose(fn func()) {
	c.closes.Append(fn)
}

func (e *env) Data() concurrent.Map {
	return e.data
}

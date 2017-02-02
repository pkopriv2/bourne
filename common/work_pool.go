package common

import (
	"time"

	"github.com/pkg/errors"
)

type WorkPool interface {
	Submit(func()) error
	SubmitOrCancel(<-chan struct{}, func()) error
	SubmitTimeout(time.Duration, func()) error
	Close() error
}

type pool struct {
	ctrl   Control
	size   int
	active chan struct{}
}

func NewWorkPool(ctrl Control, size int) WorkPool {
	if size <= 0 {
		panic("Cannot initialize an empty work pool.")
	}

	return &pool{
		ctrl:   ctrl.Sub(),
		size:   size,
		active: make(chan struct{}, size)}
}

func (p *pool) push(cancel <-chan struct{}) error {
	select {
	case <-p.ctrl.Closed():
		return errors.WithStack(ClosedError)
	case <-cancel:
		return errors.WithStack(CanceledError)
	case p.active <- struct{}{}:
		return nil
	}
}

func (p *pool) pop() {
	<-p.active
}

func (p *pool) Submit(fn func()) error {
	if err := p.push(nil); err != nil {
		return err
	}
	go func() {
		defer p.pop()
		fn()
	}()
	return nil
}

func (p *pool) SubmitOrCancel(cancel <-chan struct{}, fn func()) (err error) {
	if err := p.push(cancel); err != nil {
		return err
	}
	go func() {
		defer p.pop()
		fn()
	}()
	return nil
}

func (p *pool) SubmitTimeout(dur time.Duration, fn func()) (err error) {
	timer := time.NewTimer(dur)
	select {
	case <-p.ctrl.Closed():
		return errors.WithStack(ClosedError)
	case <-timer.C:
		return errors.WithStack(TimeoutError)
	case p.active <- struct{}{}:
	}

	go func() {
		defer p.pop()
		fn()
	}()
	return nil
}

func (p *pool) Close() error {
	return p.ctrl.Close()
}

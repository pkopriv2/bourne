package concurrent

import "errors"

var PoolClosedError = errors.New("Pool closed")

type WorkPool interface {
	Submit(func()) error
	Close() error
}

type pool struct {
	size   int
	active chan struct{}
	closed chan struct{}
	closer chan struct{}
}

func NewWorkPool(size int) WorkPool {
	if size <= 0 {
		panic("Cannot initialize an empty work pool.")
	}

	return &pool{
		size:   size,
		active: make(chan struct{}, size),
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1)}
}

func (p *pool) push() error {
	select {
	case <-p.closed:
		return PoolClosedError
	case p.active <- struct{}{}:
		return nil
	}
}

func (p *pool) pop() {
	<-p.active
}

func (p *pool) Submit(fn func()) error {
	p.push()
	go func() {
		defer p.pop()
		fn()
	}()
	return nil
}

func (p *pool) Close() error {
	select {
	case <-p.closed:
		return PoolClosedError
	case p.closer <- struct{}{}:
	}

	// wait on the active routines
	for i := 0; i < p.size; i++ {
		p.push()
	}

	close(p.closed)
	return nil
}

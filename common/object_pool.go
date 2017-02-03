package common

import (
	"container/list"
	"io"
	"time"
)

type ObjectPool interface {
	io.Closer
	Max() int
	Take() io.Closer
	TakeTimeout(time.Duration) io.Closer
	TakeOrCancel(<-chan struct{}) io.Closer
	Return(io.Closer)
	Fail(io.Closer)
}

type objectPool struct {
	ctrl   Control
	logger Logger
	fn     func() (io.Closer, error)
	raw    *list.List
	max    int
	take   chan io.Closer
	ret    chan io.Closer
}

func NewObjectPool(ctx Context, max int, fn func() (io.Closer, error)) ObjectPool {
	ctx = ctx.Sub("")
	p := &objectPool{
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
		fn:     fn,
		max:    max,
		raw:    list.New(),
		take:   make(chan io.Closer),
		ret:    make(chan io.Closer, max),
	}
	ctx.Control().Defer(func(error) {
		p.closePool()
	})

	p.start()
	return p
}

func (p *objectPool) start() {
	go func() {
		var take chan io.Closer
		var next io.Closer
		for out := 0; ; {
			// p.logger.Debug("Currently live objects [%v]", out)

			take = nil
			next = nil
			if out < p.max {
				for next == nil {
					if p.ctrl.IsClosed() {
						return
					}

					next, _ = p.takeOrSpawnFromPool()
				}
				take = p.take
			}

			select {
			case <-p.ctrl.Closed():
				if next != nil {
					next.Close()
				}
				return
			case take <- next:
				out++
			case conn := <-p.ret:
				out--
				if conn != nil {
					p.returnToPool(conn)
				}
			}
		}
	}()
}

func (p *objectPool) Max() int {
	return p.max
}

func (p *objectPool) Close() error {
	return p.ctrl.Close()
}

func (p *objectPool) Take() io.Closer {
	select {
	case <-p.ctrl.Closed():
		return nil
	case conn := <-p.take:
		return conn
	}
}

func (p *objectPool) TakeOrCancel(cancel <-chan struct{}) (conn io.Closer) {
	select {
	case <-cancel:
		return nil
	case <-p.ctrl.Closed():
		return nil
	case conn := <-p.take:
		return conn
	}
}

func (p *objectPool) TakeTimeout(dur time.Duration) (conn io.Closer) {
	timer := time.NewTimer(dur)
	select {
	case <-timer.C:
		return nil
	case <-p.ctrl.Closed():
		return nil
	case conn := <-p.take:
		return conn
	}
}

func (p *objectPool) Fail(c io.Closer) {
	c.Close()
	select {
	case <-p.ctrl.Closed():
	case p.ret <- nil:
	}
}

func (p *objectPool) Return(c io.Closer) {
	select {
	case <-p.ctrl.Closed():
	case p.ret <- c:
	}
}

func (p *objectPool) spawn() (io.Closer, error) {
	return p.fn()
}

func (p *objectPool) closePool() (err error) {
	for item := p.raw.Front(); item != nil; item = p.raw.Front() {
		err = item.Value.(io.Closer).Close()
	}
	return
}

func (p *objectPool) returnToPool(c io.Closer) {
	p.raw.PushFront(c)
}

func (p *objectPool) takeOrSpawnFromPool() (io.Closer, error) {
	if item := p.raw.Front(); item != nil {
		p.raw.Remove(item)
		return item.Value.(io.Closer), nil
	}

	return p.spawn()
}

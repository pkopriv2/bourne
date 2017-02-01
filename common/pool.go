package common

import (
	"container/list"
	"io"
	"time"
)

type ObjectPool interface {
	io.Closer
	Max() int
	Take() interface{}
	TakeTimeout(time.Duration) interface{}
	Return(interface{})
	Fail(interface{})
}

type pool struct {
	ctrl   Control
	logger Logger
	fn     func() (interface{}, error)
	raw    *list.List
	max    int
	take   chan interface{}
	ret    chan interface{}
}

func NewObjectPool(ctx Context, name string, fn func() (interface{}, error), max int) ObjectPool {
	ctx = ctx.Sub("ObjectPool(%v)", name)
	p := &pool{
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
		fn:     fn,
		max:    max,
		raw:    list.New(),
		take:   make(chan interface{}),
		ret:    make(chan interface{}, max),
	}

	p.start()
	return p
}

func (p *pool) start() {
	go func() {
		var take chan interface{}
		var next interface{}
		for out := 0; ; {
			p.logger.Debug("Currently live objects [%v]", out)

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

func (p *pool) Max() int {
	return p.max
}

func (p *pool) Close() error {
	return p.ctrl.Close()
}

func (p *pool) Take() interface{} {
	select {
	case <-p.ctrl.Closed():
		return nil
	case conn := <-p.take:
		return conn
	}
}

func (p *pool) TakeTimeout(dur time.Duration) (conn interface{}) {
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

func (p *pool) Fail(c interface{}) {
	select {
	case <-p.ctrl.Closed():
	case p.ret <- nil:
	}
}

func (p *pool) Return(c interface{}) {
	select {
	case <-p.ctrl.Closed():
	case p.ret <- c:
	}
}

func (p *pool) spawn() (interface{}, error) {
	return p.fn()
}

func (p *pool) returnToPool(c interface{}) {
	p.raw.PushFront(c)
}

func (p *pool) takeOrSpawnFromPool() (interface{}, error) {
	if item := p.raw.Front(); item != nil {
		p.raw.Remove(item)
		return item.Value.(interface{}), nil
	}

	return p.spawn()
}

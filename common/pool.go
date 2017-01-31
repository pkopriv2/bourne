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
	Return(io.Closer)
	Fail(io.Closer)
}

type pool struct {
	ctrl   Control
	logger Logger
	fn     func() (io.Closer, error)
	raw    *list.List
	max    int
	take   chan io.Closer
	ret    chan io.Closer
}

func NewObjectPool(ctx Context, name string, fn func() (io.Closer, error), max int) ObjectPool {
	ctx = ctx.Sub("ObjectPool(%v)", name)
	p := &pool{
		ctrl: ctx.Control(),
		fn:   fn,
		max:  max,
		raw:  list.New(),
		take: make(chan io.Closer),
		ret:  make(chan io.Closer, max),
	}

	p.start()
	return p
}

func (p *pool) start() {
	go func() {
		out := 0

		var take chan io.Closer
		var next io.Closer
		for {
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

			p.logger.Debug("Currently live objects [%v]", out)
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

func (p *pool) Take() io.Closer {
	select {
	case <-p.ctrl.Closed():
		return nil
	case conn := <-p.take:
		return conn
	}
}

func (p *pool) TakeTimeout(dur time.Duration) (conn io.Closer) {
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

func (p *pool) Fail(c io.Closer) {
	defer c.Close()
	select {
	case <-p.ctrl.Closed():
	case p.ret <- nil:
	}
}

func (p *pool) Return(c io.Closer) {
	select {
	case <-p.ctrl.Closed():
	case p.ret <- c:
	}
}

func (p *pool) spawn() (io.Closer, error) {
	return p.fn()
}

func (p *pool) returnToPool(c io.Closer) {
	p.raw.PushFront(c)
}

func (p *pool) takeOrSpawnFromPool() (io.Closer, error) {
	if item := p.raw.Front(); item != nil {
		p.raw.Remove(item)
		return item.Value.(io.Closer), nil
	}

	return p.spawn()
}

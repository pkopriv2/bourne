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
	ctrl Control
	fn   func() (io.Closer, error)
	raw  *list.List
	max  int
	take chan *Request
	ret  chan io.Closer
}

func NewObjectPool(ctrl Control, max int, fn func() (io.Closer, error)) ObjectPool {
	p := &objectPool{
		ctrl: ctrl.Sub(),
		fn:   fn,
		max:  max,
		raw:  list.New(),
		take: make(chan *Request),
		ret:  make(chan io.Closer, max),
	}
	ctrl.Defer(func(error) {
		p.closePool()
	})

	p.start()
	return p
}

func (p *objectPool) start() {
	go func() {
		var take chan *Request
		var next io.Closer
		for out := 0; ; {
			take = nil
			if out < p.max {
				take = p.take
			}

			select {
			case <-p.ctrl.Closed():
				if next != nil {
					next.Close()
				}
				return
			case conn := <-p.ret:
				out--
				if conn != nil {
					p.returnToPool(conn)
				}
			case req := <-take:
				out++
				req.Return(p.takeOrSpawnFromPool())
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
	if raw, err := SendRequest(p.ctrl, p.take, p.ctrl.Closed(), nil); err == nil {
		return raw.(io.Closer)
	}
	return nil
}

func (p *objectPool) TakeOrCancel(cancel <-chan struct{}) (conn io.Closer) {
	if raw, err := SendRequest(p.ctrl, p.take, cancel, nil); err == nil {
		return raw.(io.Closer)
	}
	return nil
}

func (p *objectPool) TakeTimeout(dur time.Duration) (conn io.Closer) {
	timer := NewTimer(p.ctrl, dur)
	defer timer.Close()

	if raw, err := SendRequest(p.ctrl, p.take, timer.Closed(), nil); err == nil {
		return raw.(io.Closer)
	}
	return nil
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

package net

import (
	"container/list"
	"io"
	"time"
)

// implements a simple connection pool.
// TODO: Implement TTL scheme.

type ConnectionPool interface {
	io.Closer
	Max() int
	Take() Connection
	TakeTimeout(time.Duration) Connection
	Return(Connection)
	Fail(Connection)
}

type pool struct {
	network Network
	addr    string
	timeout time.Duration
	max     int
	conns   *list.List
	take    chan Connection
	ret     chan Connection
	closed  chan struct{}
	closer  chan struct{}
}

func NewConnectionPool(network Network, addr string, max int, timeout time.Duration) ConnectionPool {
	p := &pool{
		network: network,
		addr:    addr,
		timeout: timeout,
		max:     max,
		conns:   list.New(),
		take:    make(chan Connection),
		ret:     make(chan Connection, max),
		closed:  make(chan struct{}),
		closer:  make(chan struct{}, 1),
	}

	p.start()
	return p
}

func (p *pool) start() {
	go func() {
		defer p.closePool()

		out := 0

		var take chan Connection
		var next Connection
		// var err error
		for {
			take = nil
			next = nil
			if out < p.max {
				for next == nil {
					next, _ = p.takeOrSpawnFromPool()
				}
				take = p.take
			}

			select {
			case <-p.closed:
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
	select {
	case <-p.closed:
		return ClosedError
	case p.closer <- struct{}{}:
	}

	close(p.closed)
	return nil
}

func (p *pool) Take() Connection {
	select {
	case <-p.closed:
		return nil
	case conn := <-p.take:
		return conn
	}
}

func (p *pool) TakeTimeout(dur time.Duration) (conn Connection) {
	timer := time.NewTimer(dur)
	select {
	case <-timer.C:
		return nil
	case <-p.closed:
		return nil
	case conn := <-p.take:
		return conn
	}
}

func (p *pool) Fail(c Connection) {
	defer c.Close()
	select {
	case <-p.closed:
	case p.ret <- nil:
	}
}

func (p *pool) Return(c Connection) {
	select {
	case <-p.closed:
	case p.ret <- c:
	}
}

func (p *pool) closePool() (err error) {
	for item := p.conns.Front(); item != nil; item = p.conns.Front() {
		item.Value.(io.Closer).Close()
	}
	return
}

func (p *pool) spawn() (Connection, error) {
	return p.network.Dial(p.timeout, p.addr)
}

func (p *pool) returnToPool(c Connection) {
	p.conns.PushFront(c)
}

func (p *pool) takeOrSpawnFromPool() (Connection, error) {
	if item := p.conns.Front(); item != nil {
		p.conns.Remove(item)
		return item.Value.(Connection), nil
	}

	return p.spawn()
}

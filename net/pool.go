package net

import (
	"container/list"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pkopriv2/bourne/common"
)

// implements a simple connection pool.
// TODO: Implement TTL scheme.

type ConnectionPool interface {
	io.Closer
	Max() int
	Take() Connection
	TakeTimeout(time.Duration) Connection
}

type pool struct {
	network string
	addr    string
	timeout time.Duration
	max     int
	conns   *list.List
	take    chan Connection
	ret     chan Connection
	closed  chan struct{}
	closer  chan struct{}
}

func NewConnectionPool(network string, addr string, max int, timeout time.Duration) ConnectionPool {
	p := &pool{
		network: network,
		addr:    addr,
		timeout: timeout,
		max:     max,
		conns:   list.New(),
		take:    make(chan Connection),
		ret:     make(chan Connection, 8),
		closed:  make(chan struct{}),
		closer:  make(chan struct{}, 1),
	}

	p.start()
	return p
}

func (p *pool) start() {
	go func() {
		out := 0

		var take chan Connection
		var next Connection
		for {
			take = nil
			if out < p.max {
				next, _ = p.takeOrSpawn()
				if next != nil {
					take = p.take
				}
			}
			select {
			case <-p.closed:
				return
			case take <- next:
				out++
			case conn := <-p.ret:
				out--

				if conn != nil {
					p.returnn(conn)
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
		return newPooledConn(p, conn)
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
		return newPooledConn(p, conn)
	}
}

func (p *pool) Fail(c Connection) {
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

func (p *pool) spawn() (Connection, error) {
	return net.DialTimeout(p.network, p.addr, p.timeout)
}

func (p *pool) returnn(c Connection) {
	p.conns.PushFront(c)
}

func (p *pool) takeOrSpawn() (Connection, error) {
	if item := p.conns.Front(); item != nil {
		return item.Value.(Connection), nil
	}

	return p.spawn()
}

type pooledConn struct {
	pool *pool
	raw  Connection
	lock sync.RWMutex
	err  error
}

func newPooledConn(pool *pool, raw Connection) *pooledConn {
	return &pooledConn{
		pool: pool,
		raw:  raw,
	}
}

func (p *pooledConn) Read(buf []byte) (n int, err error) {
	if err := p.ensureOpen(); err != nil {
		return 0, err
	}
	defer common.RunIf(func() { p.shutdown(err) })(err)
	n, err = p.raw.Read(buf)
	return
}

func (p *pooledConn) Write(buf []byte) (n int, err error) {
	if err := p.ensureOpen(); err != nil {
		return 0, err
	}
	defer common.RunIf(func() { p.shutdown(err) })(err)
	n, err = p.raw.Write(buf)
	return
}

func (p *pooledConn) ensureOpen() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.err
}

func (p *pooledConn) shutdown(err error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.err != nil {
		return
	}

	if err == nil {
		p.pool.Return(p.raw)
		p.err = ClosedError
	} else {
		p.pool.Fail(p.raw)
		p.err = err
	}
}

func (p *pooledConn) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.err != nil {
		return p.err
	}

	p.pool.Return(p.raw)
	p.err = ClosedError
	return nil
}

func (p *pooledConn) LocalAddr() net.Addr {
	return p.raw.LocalAddr()
}

func (p *pooledConn) RemoteAddr() net.Addr {
	return p.raw.RemoteAddr()
}

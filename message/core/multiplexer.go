package core

import (
	"github.com/pkopriv2/bourne/circuit"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/message/wire"
)

const (
	confMuxTxSize       = "bourne.msg.mux.tx.size"
	confMuxRxSize       = "bourne.msg.mux.rx.size"
	confMuxRetSize      = "bourne.msg.mux.ret.size"
	confMuxSocketRxSize = "bourne.msg.mux.socket.size"
	confMuxNumRouters   = "bourne.msg.mux.num.routers"
)

const (
	defaultMuxTxSize       = 4096
	defaultMuxRxSize       = 4096
	defaultMuxRetSize      = 128
	defaultMuxSocketRxSize = 1024
	defaultMuxNumRouters   = 3
)

type Multiplexer interface {
	Context() common.Context

	Rx() <-chan wire.Packet
	Tx() chan<- wire.Packet
	Return() <-chan wire.Packet

	AddSocket(addr interface{}) (StandardSocket, error)

	Close() error
	Fail(error)
}

type mux struct {
	ctx     common.Context
	ctrl    circuit.Controller
	ret     chan wire.Packet
	rx      chan wire.Packet
	tx      chan wire.Packet
	sockets concurrent.Map
}

func NewMultiplexer(ctx common.Context, router Router) Multiplexer {
	conf := ctx.Config()
	m := &mux{
		ctx:     ctx,
		ctrl:    circuit.NewController(),
		ret:     make(chan wire.Packet, conf.OptionalInt(confMuxRetSize, defaultMuxRetSize)),
		rx:      make(chan wire.Packet, conf.OptionalInt(confMuxRxSize, defaultMuxRxSize)),
		tx:      make(chan wire.Packet, conf.OptionalInt(confMuxTxSize, defaultMuxTxSize)),
		sockets: concurrent.NewMap(),
	}

	numRouters := ctx.Config().OptionalInt(confMuxNumRouters, defaultMuxNumRouters)
	for i := 0; i < numRouters; i++ {
		ctrl, _ := m.ctrl.NewControlSocket()
		go multiplexerRoute(m, ctrl, router)
	}

	return m
}

func (m *mux) NewControlSocket() (circuit.ControlSocket, error) {
	return m.ctrl.NewControlSocket()
}

func (m *mux) Context() common.Context {
	return m.ctx
}

func (m *mux) Close() error {
	return m.ctrl.Close()
}

func (m *mux) Fail(e error) {
	m.ctrl.Fail(e)
}

func (m *mux) Return() <-chan wire.Packet {
	return m.ret
}

func (m *mux) Rx() <-chan wire.Packet {
	return m.rx
}

func (m *mux) Tx() chan<- wire.Packet {
	return m.tx
}

func (m *mux) AddSocket(addr interface{}) (StandardSocket, error) {
	socket, err := newMuxSocket(m, addr)
	if err != nil {
		return nil, err
	}

	if err := m.sockets.Put(addr, socket); err != nil {
		return nil, err
	}

	return socket, nil
}

func multiplexerReturn(m *mux, ctrl circuit.ControlSocket, p wire.Packet) bool {
	select {
	case <-ctrl.Failed():
		return false
	case <-ctrl.Closed():
		return false
	case m.ret <- p:
		return true
	}
}

func multiplexerRoute(m *mux, ctrl circuit.ControlSocket, fn Router) {
	defer ctrl.Done()
	for {
		var p wire.Packet
		select {
		case <-ctrl.Failed():
			return
		case <-ctrl.Closed():
			return
		case p = <-m.tx:
		}

		addr := fn(p)
		if addr == nil {
			if !multiplexerReturn(m, ctrl, p) {
				return
			}

			continue
		}

		dst := m.sockets.Get(addr)
		if dst == nil {
			if !multiplexerReturn(m, ctrl, p) {
				return
			}

			continue
		}

		socket := dst.(*muxSocket)
		select {
		case <-ctrl.Failed():
			return
		case <-ctrl.Closed():
			return
		case socket.rx <- p:
		}
	}
}

type muxSocket struct {
	mux  *mux
	addr interface{}
	ctrl circuit.ControlSocket
	rx   chan wire.Packet
}

func newMuxSocket(mux *mux, addr interface{}) (*muxSocket, error) {
	conf := mux.Context().Config()

	ctrl, err := mux.ctrl.NewControlSocket()
	if err != nil {
		return nil, err
	}

	return &muxSocket{
		mux:  mux,
		addr: addr,
		ctrl: ctrl,
		rx:   make(chan wire.Packet, conf.OptionalInt(confMuxSocketRxSize, defaultMuxSocketRxSize))}, nil

}

func (m *muxSocket) Closed() <-chan struct{} {
	return m.ctrl.Closed()
}

func (m *muxSocket) Failed() <-chan struct{} {
	return m.ctrl.Failed()
}

func (m *muxSocket) Failure() error {
	return m.ctrl.Failure()
}

func (m *muxSocket) Done() {
	m.mux.sockets.Remove(m.addr)
	m.ctrl.Done()
}

func (m *muxSocket) Rx() <-chan wire.Packet {
	return m.rx
}

func (m *muxSocket) Tx() chan<- wire.Packet {
	return m.mux.tx
}

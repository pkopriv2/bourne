package core

import (
	"github.com/pkopriv2/bourne/circuit"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/message/wire"
)

type DirectTopology interface {
	Context() common.Context

	SocketL() (StandardSocket, error)
	SocketR() (StandardSocket, error)

	Close() error
	Fail(error)
}

type direct struct {
	ctx  common.Context
	ctrl circuit.Controller

	chanL chan wire.Packet // 1 -> 2
	chanR chan wire.Packet // 2 -> 1

	socketL StandardSocket // transient
	socketR StandardSocket // transient
}

func NewDirectTopology(ctx common.Context) DirectTopology {
	return &direct{
		ctx:   ctx,
		ctrl:  circuit.NewController(),
		chanL: make(chan wire.Packet),
		chanR: make(chan wire.Packet)}
}

func route(rx <-chan wire.Packet, tx chan<- wire.Packet, ctrl circuit.ControlSocket) {
	defer ctrl.Done()

	chanIn := rx
	chanOut := tx

	for {
		var p wire.Packet
		select {
		case <-ctrl.Closed():
			return
		case <-ctrl.Failed():
			return
		case p = <-chanIn:
		}

		select {
		case <-ctrl.Closed():
			return
		case <-ctrl.Failed():
			return
		case chanOut <- p:
		}
	}
}

func (c *direct) Context() common.Context {
	return c.ctx
}

func (c *direct) SocketR() (StandardSocket, error) {
	if c.socketR != nil {
		return c.socketR, nil
	}

	ctrl, err := c.ctrl.NewControlSocket()
	if err != nil {
		return nil, err
	}

	c.socketR = NewStandardSocket(ctrl, c.chanR, c.chanL)
	return c.socketR, nil
}

func (c *direct) SocketL() (StandardSocket, error) {
	if c.socketL != nil {
		return c.socketL, nil
	}

	ctrl, err := c.ctrl.NewControlSocket()
	if err != nil {
		return nil, err
	}

	c.socketL = NewStandardSocket(ctrl, c.chanL, c.chanR)
	return c.socketL, nil
}

func (c *direct) Close() error {
	return c.ctrl.Close()
}

func (c *direct) Fail(e error) {
	c.ctrl.Fail(e)
}

type directSocket struct {
	ctrl circuit.ControlSocket
	rx   <-chan wire.Packet
	tx   chan<- wire.Packet
}

func newDirectSocket(ctrl circuit.ControlSocket, rx <-chan wire.Packet, tx chan<- wire.Packet) StandardSocket {
	return &directSocket{ctrl, rx, tx}
}

func (c *directSocket) Closed() <-chan struct{} {
	return c.ctrl.Closed()
}

func (c *directSocket) Failed() <-chan struct{} {
	return c.ctrl.Failed()
}

func (c *directSocket) Failure() error {
	return c.ctrl.Failure()
}

func (c *directSocket) Done() {
	c.ctrl.Done()
}

func (c *directSocket) Rx() <-chan wire.Packet {
	return c.rx
}

func (c *directSocket) Tx() chan<- wire.Packet {
	return c.tx
}

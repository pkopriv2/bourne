package core

import (
	"github.com/pkopriv2/bourne/circuit"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/message/wire"
)

type Crossover interface {
	Context() common.Context

	L() StandardSocket
	R() StandardSocket

	Close() error
	Fail(error)
}

type crossover struct {
	ctx  common.Context
	ctrl circuit.Controller

	l StandardSocket
	r StandardSocket
}

func NewCrossover(ctx common.Context) Crossover {
	chanL := make(chan wire.Packet)
	chanR := make(chan wire.Packet)

	ctrl := circuit.NewController()

	ctrlL, _ := ctrl.NewControlSocket()
	ctrlR, _ := ctrl.NewControlSocket()

	l := newCrossoverSocket(ctrlL, chanL, chanR)
	r := newCrossoverSocket(ctrlR, chanR, chanL)

	c := &crossover{ctx, ctrl, l, r}

	ctrlRouteL, _ := ctrl.NewControlSocket()
	go route(chanL, chanR, ctrlRouteL)

	ctrlRouteR, _ := ctrl.NewControlSocket()
	go route(chanR, chanL, ctrlRouteR)

	return c
}

func route(rx <-chan wire.Packet, tx chan<- wire.Packet, ctrl circuit.ControlSocket) {
	defer ctrl.Done()

	var p wire.Packet

	chanIn := rx
	chanOut := tx
	for {

		if p == nil {
			chanIn = rx
			chanOut = nil
		} else {
			chanIn = nil
			chanOut = tx
		}

		select {
		case <-ctrl.Closed():
			return
		case <-ctrl.Failed():
			return
		case p = <- chanIn:
		case chanOut <- p:
		}
	}
}

func (c *crossover) Context() common.Context {
	return c.ctx
}

func (c *crossover) L() StandardSocket {
	return c.l
}

func (c *crossover) R() StandardSocket {
	return c.r
}

func (c *crossover) Close() error {
	return c.ctrl.Close()
}

func (c *crossover) Fail(e error) {
	c.ctrl.Fail(e)
}

type crossoverSocket struct {
	ctrl circuit.ControlSocket
	rx   <-chan wire.Packet
	tx   chan<- wire.Packet
}

func newCrossoverSocket(ctrl circuit.ControlSocket, rx <-chan wire.Packet, tx chan<- wire.Packet) StandardSocket {
	return &crossoverSocket{ctrl, rx, tx}
}

func (c *crossoverSocket) Closed() <-chan struct{} {
	return c.ctrl.Closed()
}

func (c *crossoverSocket) Failed() <-chan struct{} {
	return c.ctrl.Failed()
}

func (c *crossoverSocket) Failure() error {
	return c.ctrl.Failure()
}

func (c *crossoverSocket) Done() {
	c.ctrl.Done()
}

func (c *crossoverSocket) Rx() <-chan wire.Packet {
	return c.rx
}

func (c *crossoverSocket) Tx() chan<- wire.Packet {
	return c.tx
}

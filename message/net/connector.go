package net

import (
	"bufio"
	"errors"
	"io"

	"github.com/pkopriv2/bourne/circuit"
	"github.com/pkopriv2/bourne/message/core"
	"github.com/pkopriv2/bourne/message/wire"
	"github.com/pkopriv2/bourne/net"
)

var ConnectorClosedError = errors.New("ERR:CONNECTOR:CLOSED")

type Connector interface {
	core.StandardSocket
	io.Closer
}

type connector struct {
	raw    net.Connection
	ctrl   circuit.Controller
	socket circuit.ControlSocket
	rx     chan wire.Packet
	tx     chan wire.Packet
	fail   chan error
	close  chan struct{}
}

func NewConnector(raw net.Connection) Connector {
	ctrl := circuit.NewController()
	sock, _ := ctrl.NewControlSocket()

	c := &connector{
		raw:    raw,
		socket: sock,
		rx:     make(chan wire.Packet),
		tx:     make(chan wire.Packet),
		fail:   make(chan error),
		close:  make(chan struct{})}

	r, _ := c.ctrl.NewControlSocket()
	w, _ := c.ctrl.NewControlSocket()
	go connectorControl(c)
	go connectorReader(c, r)
	go connectorWriter(c, w)

	return c
}

func (c *connector) Close() error {
	select {
	case <-c.socket.Closed():
		return ConnectorClosedError
	case <-c.socket.Failed():
		return ConnectorClosedError
	case c.close <- struct{}{}:
	}

	return c.socket.Failure()
}

func (c *connector) Closed() <-chan struct{} {
	return c.socket.Closed()
}

func (c *connector) Failed() <-chan struct{} {
	return c.socket.Failed()
}

func (c *connector) Failure() error {
	return c.socket.Failure()
}

func (c *connector) Done() {
	c.socket.Done()
}

func (c *connector) Rx() <-chan wire.Packet {
	return c.rx
}

func (c *connector) Tx() chan<- wire.Packet {
	return c.tx
}

func connectorControl(c *connector) {
	select {
	case <-c.close:
		c.ctrl.Close()
		c.raw.Close() // TODO: should close raw on error too?
	case err := <-c.fail:
		c.ctrl.Fail(err)
	}
}

func fail(c *connector, ctrl circuit.ControlSocket, err error) {
	select {
	case <-ctrl.Closed():
	case <-ctrl.Failed():
	case c.fail <- err:
	}
}

func connectorReader(c *connector, ctrl circuit.ControlSocket) {
	defer ctrl.Done()

	reader := bufio.NewReader(c.raw)
	for {
		p, err := wire.ReadPacket(reader)
		if err != nil {
			switch t := err.(type) {
			case net.ConnectionError:
				fail(c, ctrl, t)
				return
			}
		}

		select {
		case <-ctrl.Closed():
			return
		case <-ctrl.Failed():
			return
		case c.rx <- p:
		}
	}
}

func connectorWriter(c *connector, ctrl circuit.ControlSocket) {
	defer ctrl.Done()

	writer := bufio.NewWriter(c.raw)
	for {
		var p wire.Packet

		select {
		case <-ctrl.Closed():
			return
		case <-ctrl.Failed():
			return
		case p = <-c.tx:
		}

		if err := p.Write(writer); err != nil {
			switch e := err.(type) {
			case net.ConnectionError:
				fail(c, ctrl, e)
				return
			}
		}
	}
}

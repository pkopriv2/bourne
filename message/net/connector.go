package net

import (
	"bufio"
	"sync"

	"github.com/pkopriv2/bourne/msg/core"
	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/utils"
)

type Connector interface {
	core.StandardSocket
}

type connector struct {
	connection  net.Connection
	rx    chan wire.Packet
	tx    chan wire.Packet
	err   chan error
	wait  sync.WaitGroup
}

func (c *connector) Closed() <-chan struct{} {
	return c.ctrl.Closed()
}

func (c *connector) Failed() <-chan struct{} {
	return c.ctrl.Failed()
}

func (c *connector) Failure() error {
	return c.
	panic("not implemented")
}

func (c *connector) Done() {
	panic("not implemented")
}

func (c *connector) Return() <-chan wire.Packet {
	panic("not implemented")
}

func (c *connector) Rx() <-chan wire.Packet {
	panic("not implemented")
}

func (c *connector) Tx() chan<- wire.Packet {
	panic("not implemented")
}

func NewConnector(config utils.Config, conn net.Connection) Connector {
	c := &connector{
		conn: conn,
		rx:   make(chan wire.Packet),
		tx:   make(chan wire.Packet),
		err:  make(chan error, 2)}

	// start reader
	c.wait.Add(1)
	go func() {
		defer c.wait.Done()
		reader := bufio.NewReader(c.conn)

		for {
			p, err := wire.ReadPacket(reader)
			if err != nil {
				switch t := err.(type) {
				case net.ConnectionError:
					c.err <- t
					return
				}
			}

			select {
			case <-c.close:
				return
			case c.rx <- p:
			}
		}
	}()

	// start writer
	c.wait.Add(1)
	go func() {
		defer c.wait.Done()
		writer := bufio.NewWriter(c.conn)

		for {
			var p wire.Packet

			select {
			case <-c.close:
				return
			case p = <-c.tx:
			}

			if err := p.Write(writer); err != nil {
				switch t := err.(type) {
				case net.ConnectionError:
					c.err <- t
					return
				}
			}
		}
	}()

	return c
}

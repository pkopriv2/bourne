package core

import (
	"sync"

	"github.com/pkopriv2/bourne/msg/net"
	"github.com/pkopriv2/bourne/msg/wire"
)

type Connector interface {
	ControlSocket
}

type connector struct {
	conn  net.Connection
	rx    chan wire.Packet
	tx    chan wire.Packet
	err   chan error
	close chan struct{}
	wait  sync.WaitGroup
}

func (c *connector) Close() {
	close(c.close)
}

func (c *connector) Rx() <-chan wire.Packet {
	return c.rx
}

func (c *connector) Tx() chan<- wire.Packet {
	return c.tx
}

func (c *connector) Err() <-chan error {
	return c.err
}

// func NewConnector(config utils.Config, conn net.Connection, ControlSocket) Connector {
// c := &connector{
// conn: conn,
// rx:   make(chan wire.Packet),
// tx:   make(chan wire.Packet),
// err:  make(chan error, 2)}
//
// // start reader
// c.wait.Add(1)
// go func() {
// defer c.wait.Done()
// reader := bufio.NewReader(c.conn)
//
// for {
// p, err := wire.ReadPacket(reader)
// if err != nil {
// switch t := err.(type) {
// case net.ConnectionError:
// c.err <- t
// return
// }
// }
//
// select {
// case <-c.close:
// return
// case c.rx <- p:
// }
// }
// }()
//
// // start writer
// c.wait.Add(1)
// go func() {
// defer c.wait.Done()
// writer := bufio.NewWriter(c.conn)
//
// for {
// var p wire.Packet
//
// select {
// case <-c.close:
// return
// case p = <-c.tx:
// }
//
// if err := p.Write(writer); err != nil {
// switch t := err.(type) {
// case net.ConnectionError:
// c.err <- t
// return
// }
// }
// }
// }()
//
// return c
// }

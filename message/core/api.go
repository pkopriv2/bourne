package core

import (
	"github.com/pkopriv2/bourne/circuit"
	"github.com/pkopriv2/bourne/message/wire"
)

type Router func(wire.Packet) (addr interface{})

type DataSocket interface {
	Rx() <-chan wire.Packet
	Tx() chan<- wire.Packet
}

type StandardSocket interface {
	circuit.ControlSocket
	DataSocket
}

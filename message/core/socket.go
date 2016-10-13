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

type ReliableDataSocket interface {
	DataSocket
	Return() <-chan wire.Packet
}

type StandardSocket interface {
	circuit.ControlSocket
	DataSocket
}

type DataDriver interface {
	DataSocket

	RxDriver() chan<- wire.Packet
	TxDriver() <-chan wire.Packet
}

type dataDriver struct {
	rx chan wire.Packet
	tx chan wire.Packet
}

func NewDataDriver(rxBuf int, txBuf int) DataDriver {
	return &dataDriver{make(chan wire.Packet), make(chan wire.Packet)}
}

func (d *dataDriver) Rx() <-chan wire.Packet {
	return d.rx
}

func (d *dataDriver) Tx() chan<- wire.Packet {
	return d.tx
}

func (d *dataDriver) RxDriver() chan<- wire.Packet {
	return d.rx
}

func (d *dataDriver) TxDriver() <-chan wire.Packet {
	return d.tx
}


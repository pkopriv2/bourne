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

type standardSocket struct {
	ctrl circuit.ControlSocket
	rx <-chan wire.Packet
	tx chan<- wire.Packet
}

func NewStandardSocket(ctrl circuit.ControlSocket, rx <-chan wire.Packet, tx chan<- wire.Packet) StandardSocket {
	return &standardSocket{ctrl, rx, tx}
}

func (s *standardSocket) Closed() <-chan struct{} {
	return s.ctrl.Closed()
}

func (s *standardSocket) Failed() <-chan struct{} {
	return s.ctrl.Failed()
}

func (s *standardSocket) Failure() error {
	return s.ctrl.Failure()
}

func (s *standardSocket) Done() {
	s.ctrl.Done()
}

func (s *standardSocket) Rx() <-chan wire.Packet {
	return s.rx
}

func (s *standardSocket) Tx() chan<- wire.Packet {
	return s.tx
}


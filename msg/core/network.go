package core

import "github.com/pkopriv2/bourne/msg/wire"

type ControlSocket interface {
	Close() <-chan struct{}
	Error() <-chan error
}

type ControlDriver interface {
	CloseDriver() chan<- struct{}
	ErrorDriver() chan<- error
}

type ControlSocketDriver interface {
	ControlSocket
	ControlDriver
}

type DataSocket interface {
	Rx()  <-chan wire.Packet
	Tx()  chan<- wire.Packet
}

type DataDriver interface {
	RxDriver()  chan<- wire.Packet
	TxDriver()  <-chan wire.Packet
}

type DataSocketDriver interface {
	DataSocket
	DataDriver
}

type StandardSocket interface {
	ControlSocket
	DataSocket
}

type StandardDriver interface {
	DataDriver
	ControlDriver
}

type StandardSocketDriver interface {
	StandardSocket
	StandardDriver
}

type ControlNetwork interface {
	ControlDriver
	NewController() ControlSocket
}

type Router func(wire.Packet) int

type Process func(StandardSocket)

type Bus interface {
	Id() int
	Run(Process)
}

type Driver interface {
	ControlDriver // this forces the consumer to handle wiring to input

	AddBus(int) Bus
	AddRouter(Router)
	AddInput(DataSocket)
}

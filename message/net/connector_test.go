package net

import "github.com/pkopriv2/bourne/message/wire"

type TestConnection struct {
	read     chan wire.Packet
	write    chan wire.Packet
	readErr  chan error
	writeErr chan error
}

func (t *TestConnection) Read(p []byte) (n int, err error) {
	panic("not implemented")
}

func (t *TestConnection) Write(p []byte) (n int, err error) {
	panic("not implemented")
}

func (t *TestConnection) Close() error {
	panic("not implemented")
}

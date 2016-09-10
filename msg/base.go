package msg

import "io"

type BaseChannel interface {
	io.Closer

	// Sends a packet the processors input channel.  Each implementation
	// should attempt to implement this in a NON-BLOCKING
	// fashion. However, may block if necessary.
	//
	send(p *Packet) error
}

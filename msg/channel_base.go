package msg

import "io"

type ChannelBase interface {
	io.Closer

	// Sends a packet the processors input channel.  Each implementation
	// should attempt to implement this in a NON-BLOCKING
	// fashion. However, may block if necessary.
	//
	Send(p *Packet) error
}

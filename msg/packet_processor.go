package msg

import "io"

// ** INTERNAL ONLY **
//
// The base packet processing interface
//
// Implementations must be *thread-safe*
//
type PacketProcessor interface {
	io.Closer

	// Sends a packet the processors input channel.  Each implementation
	// should attempt to implement this in a NON-BLOCKING
	// fashion. However, may block if necessary.
	//
	SendIn(p *Packet) error

	// Sends a packet the processors output channel.  Each implementation
	// should attempt to implement this in a NON-BLOCKING
	// fashion. However, may block if necessary.
	//
	SendOut(p *Packet) error
}

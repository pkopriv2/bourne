package msg

import (
	"errors"
	"log"
	"sync"
	"time"
)

const (
	ClientInit   AtomicState = 0
	ClientOpened AtomicState = 1 << iota
	ClientClosing
	ClientClosed
	ClientFailure
)

const (
	defaultClientWriterInSize   = 8192
	defaultClientRouterInSize   = 4096
	defaultClientWriterWait     = 1 * time.Millisecond
	defaultClientReaderWait     = 1 * time.Millisecond
	defaultClientRouterWait     = 10 * time.Millisecond
	defaultClientConnectRetries = 3
	defaultClientConnectTimeout = 3
)

var (
	ErrClientClosed  = errors.New("CLIENT:CLOSED")
	ErrClientFailure = errors.New("CLIENT:FAILURE")
)


type ClientOptions struct {
	Debug bool

	WriterInSize int
	RouterInSize int

	WriterWait time.Duration
	ReaderWait time.Duration
	RouterWait time.Duration

	ConnectRetries int
	ConnectTimeout time.Duration

	Factory ConnectionFactory

	ListenerOpts ListenerOptionsHandler
	ChannelOpts  ChannelOptionsHandler
}

type ClientOptionsHandler func(*ClientOptions)

// A client is responsible for taking a single data stream
// and splitting it into multiple logical streams.  Once split,
// these streams are referred to as "channels".  A channel represents
// one side of a conversation between two entities.  Channels come
// in two different flavors:
//
//      * Active - An active channel represents a "live" conversation
//        between two entities.  Channels are full duplex, meaning that they
//        have separate input and output streams.  The relationship between
//        client and server is NOT specified at this layer, but it is generally
//        perceived that a listening channel will spawn a "server" channel
//        while client channels must be spawned adhoc via the connect command.
//
//      * Listening - A listening channel spawns active channels.
//
// Data flow:
//
//  IN FLOW:
//  <DATA> ---> READER ---> ROUTER ----> *CHANNEL
//
//  OUT FLOW:
//  <DATA> ---> *CHANNEL ---> WRITER ----> <DATA>
//
// NOTE: Any reference to output refers to the OUT FLOW direction.
//
// Thrading model:
//
//      * RAW READER   : SINGLE THREADED (No one but reader should read from this)
//      * RAW WRITER   : SINGLE THREADED (No one but writer should write to this)
//      * READER       : A single instance operating in its own routine.  (always limited to a singleton)
//      * WRITER       : A single instance operating in its own routine.  (always limited to a singleton)
//      * ROUTER       : Each instance
//      * CHANNEL      : Manages its own threads.
//
// Multiplex closing:
//
//      * Invoke close on all channels
//      * Closes the Reader
//      * Closes the Writer
//
// Examples:
//   c := NewMux(...)
//   s := c.connect(0,0)
//
// Example:
//

//
type client struct {

	options *ClientOptions

	state *AtomicState

	// pool of available channel ids
	ids *idPool

	// the complete listing of sessions (both active and listening)
	router *routingTable

	// the
	conn     *Connector

	// IO
	routerIn chan *packet
	writerIn chan *packet

	// all the active routines under this multiplexer
	workers sync.WaitGroup
}

func (c *client) Close() error {
	panic("not implemented")
}

func (c *client) Spawn(entity EntityId, remote EndPoint) (Channel, error) {
	panic("not implemented")
}

func (c *client) Listen(local EndPoint) (Listener, error) {
	panic("not implemented")
}

// ** INTERNAL ONLY METHODS **

// Logs a message, tagging it with the channel's local address.
// func (c *client) log(format string, vals ...interface{}) {
	// if !c.options.Debug {
		// return
	// }
//
	// // log.Println(fmt.Sprintf("client(%v) -- ", c.entityId) + fmt.Sprintf(format, vals...))
// }

func readerWorker(c *client) {
	defer c.workers.Done()
	for {
		state := c.state.WaitUntil(ClientOpened | ClientClosing | ClientClosed)
		if !state.Is(ClientOpened) {
			return
		}

		// packet, err := ReadPacket(c.conn)
		// switch {
		// case err = *err.(ConnectionErrors):
//
		// }
		// if err != nil {
//
		// }
		// if err != nil {
			// log.Printf("Error parsing packet")
			// time.Sleep(c.options.ReaderWait)
			// continue
		// }

		// c.routerIn <- packet

	}
}

func writerWorker(c *client) {
	defer c.workers.Done()
	for {
		state := c.state.WaitUntil(ClientOpened | ClientClosing | ClientClosed)
		if !state.Is(ClientOpened) {
			return
		}

		packet, ok := <-c.writerIn
		if !ok {
			log.Println("Channel closed.  Stopping writer thread")
		}

		if err := WritePacket(c.conn, packet); err != nil {
			// TODO: Handle connection errors
			log.Printf("Error writing packet [%v]\n", err)
			continue
		}
	}
}

func routerWorker(c *client) {
	defer c.workers.Done()
	for {
		p, ok := <-c.routerIn
		if !ok {
			// todo...return error packet!
			return
		}

		// see if there is an "active" session
		var channel Routable
		if channel = c.router.Get(NewChannelSession(p.dst, p.src)); channel != nil {
			if err := channel.send(p); err != nil {
				c.writerIn <- NewReturnPacket(p, PacketFlagErr, 0, 0, []byte(err.Error()))
			}
			continue
		}

		// see if there is a "listener" session
		if channel = c.router.Get(NewListenerSession(p.dst)); channel != nil {
			if err := channel.send(p); err != nil {
				c.writerIn <- NewReturnPacket(p, PacketFlagErr, 0, 0, []byte(err.Error()))
			}
			continue
		}

		// nobody to handle this.
		c.writerIn <- NewReturnPacket(p, PacketFlagErr, 0, 0, []byte(ErrChannelUnknown.Error()))
	}
}

package msg

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

var (
	ErrClientClosed  = errors.New("CLIENT:CLOSED")
	ErrClientFailure = errors.New("CLIENT:FAILURE")
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

func Connect(EndPoint) (Channel, error) {
	return nil, nil
}

func Listen(EndPoint) (Listener, error) {
	return nil, nil
}

type ClientOptions struct {
	Debug          bool
	WriterInSize   int
	RouterInSize   int
	WriterWait     time.Duration
	ReaderWait     time.Duration
	RouterWait     time.Duration
	ConnectRetries int
	ConnectTimeout time.Duration

	ConnectionFactory ConnectionFactory

	ListenerOpts ListenerOptionsHandler
	ChannelOpts  ChannelOptionsHandler
}

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
//      * RAW READER   : SINGLE THREADED (No one but multiplexer should read from this)
//      * RAW WRITER   : SINGLE THREADED (No one but multiplexer should write to this)
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

	// the entity behind this client.
	entityId EntityId

	// client options
	options ClientOptions

	// the current "live" connection (initially nil) (synchronized via lock)
	conn *Connector

	// the current state of the client
	state *AtomicState

	// pool of available channel ids
	ids *idPool

	// the complete listing of sessions (both active and listening)
	router *routingTable

	// Direction: RECV deserializerIn --> routerIn
	routerIn chan *packet

	// Direction: SEND *Channel --> serializerIn --> serializerOut
	writerIn chan *packet

	// a flag indicating state.
	closed bool

	// the lock on the multiplexer
	lock sync.RWMutex

	// all the active routines under this multiplexer
	workers sync.WaitGroup
}

func (c *client) Close() error {
	panic("not implemented")
}

func (c *client) EntityId() uint32 {
	panic("not implemented")
}

func (c *client) Connect(remote EndPoint) (Channel, error) {
	panic("not implemented")
}

func (c *client) Listen(channelId uint32) (Listener, error) {
	panic("not implemented")
}

// ** INTERNAL ONLY METHODS **

// Logs a message, tagging it with the channel's local address.
func (c *client) log(format string, vals ...interface{}) {
	if !c.options.Debug {
		return
	}

	log.Println(fmt.Sprintf("client(%v) -- ", c.entityId) + fmt.Sprintf(format, vals...))
}

func readerWorker(c *client) {
	defer c.workers.Done()
	for {

		packet, err := ReadPacket(c.conn)
		if err != nil {
			log.Printf("Error parsing packet")
			continue
		}

		c.routerIn <- packet
	}
}

func writerWorker(c *client) {
	defer c.workers.Done()
	for {
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

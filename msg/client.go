package msg

import (
	"errors"
	"io"
	"log"
	"sync"
)

var (
	ErrConnectionFailure = errors.New("CHAN:HANDSHAKE")
	ErrConnectionTimeout = errors.New("CHAN:HANDSHAKE")
	ErrClientClosed   = errors.New("CHAN:CLOSED")
	ErrClientFailure  = errors.New("CHAN:FAILURE")
	ErrClientTimeout  = errors.New("CHAN:TIMEOUT")
	ErrClientExists   = errors.New("CHAN:EXISTS")
	ErrClientUnknown  = errors.New("CHAN:UNKNONW")
)

const (
	ClientInit AtomicState = 0
	ClientConnecting AtomicState = 1 << iota
	ClientConnected
	ClientClosing
	ClientClosed
	ClientFailure
)

const (
	// defaultChannelRecvInSize   = 1024
	// defaultChannelRecvLogSize  = 1 << 20 // 1024K
	// defaultChannelSendLogSize  = 1 << 18 // 256K
	// defaultChannelSendWait     = 100 * time.Millisecond
	// defaultChannelRecvWait     = 20 * time.Millisecond
	// defaultChannelAckTimeout   = 5 * time.Second
	// defaultChannelWinTimeout   = 2 * time.Second
	// defaultChannelCloseTimeout = 10 * time.Second
	// defaultChannelMaxRetries   = 3
)

// The primary client interface.
type Client interface {
	io.Closer

	// Connects to the remote endpoint and returns a channel.
	Connect(entityId EntityId, ChannelId uint16) (Channel, error)

	// Begins listening on
	Listen(channelId uint16) (Listener, error)
}


// A connection is a full-duplex streaming abstraction.
//
// Implementations are not explicitly required to be thread-safe.
type Connection interface {
	io.Reader
	io.Writer
	io.Closer
}

// Connection actories are used to create the underlying streams.  In
// the event of failure, this allows streams to be "recreated", without
// leaking how the streams are generated.  The intent is to create a
// highly resilient multiplexer.
//
// Consumers MUST NOT interfere with stream lifecycle.  Rather, they
// should manage the lifecycle of the multiplexer.
//
type ConnectionFactory func() (Connection, error)

// A multiplexer is responsible for taking a single data stream
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
//      * READER       : SINGLE THREADED (No one but multiplexer should read from this)
//      * WRITER       : SINGLE THREADED (No one but multiplexer should write to this)
//      * ROUTER       : A single instance operating in its own routine.(buffered input)
//      * CHANNEL      : Manages its own threads.
//
// Multiplex closing:
//
//      * Invoke close on all channels
//      * Closes the Reader
//      * Closes the Writer
//
// Examples:
//   m := NewMux(...)
//   s := m.connect(0,0)
//
// Example:
//

// the parser output buffer is shared amonst all channels
// var PARSER_BUF_OUT_SIZE uint = 8192

// In order to build a highly resilient multiplexer we will
// impement reconnect logic.
var STREAM_MAX_RECONNECTS uint = 5


//
type Mux struct {

	// the entity behind this client.
	entityId uint32

	//
	factory ConnectionFactory

	// pool of available channel ids
	ids *IdPool

	// the complete listing of sessions (both active and listening)
	router *routingTable

	// Direction: RECV deserializerIn --> routerIn
	readerIn io.Reader
	routerIn chan *Packet

	// Direction: SEND *Channel --> serializerIn --> serializerOut
	writerIn  chan *Packet
	writerOut io.Writer

	// a flag indicating state.
	closed bool

	// the lock on the multiplexer
	lock sync.RWMutex

	// all the active routines under this multiplexer
	workers sync.WaitGroup
}

func (m *Mux) Close() error {
	panic("not implemented")
}

func (m *Mux) EntityId() uint32 {
	panic("not implemented")
}

func (m *Mux) Connect(remote EndPoint) (Channel, error) {
	panic("not implemented")
}

func (m *Mux) Listen(channelId uint32) (Listener, error) {
	panic("not implemented")
}

func readerWorker(m *Mux) {
	defer m.workers.Done()
	for {

		packet, err := ReadPacket(m.readerIn)
		if err != nil {
			log.Printf("Error parsing packet")
			continue
		}

		m.routerIn <- packet
	}
}

func writerWorker(m *Mux) {
	defer m.workers.Done()
	for {
		packet, ok := <-m.writerIn
		if !ok {
			log.Println("Channel closed.  Stopping writer thread")
		}

		if err := WritePacket(m.writerOut, packet); err != nil {
			// TODO: Handle connection errors
			log.Printf("Error writing packet [%v]\n", err)
			continue
		}
	}
}

func routerWorker(m *Mux) {
	defer m.workers.Done()
	for {
		p, ok := <-m.routerIn
		if !ok {
			// todo...return error packet!
			return
		}

		// local := NewEndPoint(p.dstEntityId, p.dstChannelId)

		// see if there is an "active" session
		var channel Routable
		if channel = m.router.Get(NewChannelSession(p.dst, p.src)); channel != nil {
			if err := channel.send(p); err != nil {
				m.writerIn <- NewReturnPacket(p, PacketFlagErr, 0, 0, 0, []byte(err.Error()))
			}
			continue
		}

		// see if there is a "listener" session
		if channel = m.router.Get(NewListenerSession(p.dst)); channel != nil {
			if err := channel.send(p); err != nil {
				m.writerIn <- NewReturnPacket(p, PacketFlagErr, 0, 0, 0, []byte(err.Error()))
			}
			continue
		}

		// nobody to handle this.
		m.writerIn <- NewReturnPacket(p, PacketFlagErr, 0, 0, 0, []byte(ErrChannelUnknown.Error()))
	}
}

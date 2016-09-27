package client

import (
	"bufio"
	"errors"
	"sync"
	"time"

	"github.com/pkopriv2/bourne/msg/net"
	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
)

const (
	confClientWriterInSize   = "bourne.msg.client.writer.in.size"
	confClientRouterInSize   = "bourne.msg.client.router.in.size"
	confClientWriterWait     = "bourne.msg.client.writer.wait"
	confClientReaderWait     = "bourne.msg.client.reader.wait"
	confClientRouterWait     = "bourne.msg.client.router.wait"
	confClientConnectRetries = "bourne.msg.client.connect.attempts"
	confClientConnectTimeout = "bourne.msg.client.connect.timeout"
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

const (
	ClientRouteErrorCode = 200
)

var (
	NewRoutingError = wire.NewProtocolErrorFamily(ChannelOpeningErrorCode)
)

// const (
	// ClientOpened AtomicState = 1 << iota
	// ClientClosed
	// ClientFailure
// )

type ClientOptions struct {
	Config utils.Config

	Factory      net.ConnectionFactory
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
type client struct {
	debug bool

	options *ClientOptions

	state *interface{}

	reader *bufio.Reader
	writer *bufio.Writer

	ids    *IdPool
	router *routingTable

	routerIn chan wire.Packet
	writerIn chan wire.Packet

	workers sync.WaitGroup

	writerInSize int
	routerInSize int

	writerWait time.Duration
	readerWait time.Duration
	routerWait time.Duration

	connectRetries int
	connectTimeout time.Duration
}

func (c *client) Close() error {
	panic("not implemented")
}

// func (c *client) Spawn(entity EntityId, remote EndPoint) (Channel, error) {
// panic("not implemented")
// }
//
// func (c *client) Listen(local EndPoint) (Listener, error) {
// panic("not implemented")
// }

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
		// if state.Is(^ClientOpened) {
		// return
		// }
		//
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
		//
		// c.routerIn <- packet
	}
}


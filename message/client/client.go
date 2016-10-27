package client

import (
	"errors"
	"io"
	"time"

	"github.com/pkopriv2/bourne/circuit"
	"github.com/pkopriv2/bourne/common"
	mnet "github.com/pkopriv2/bourne/message/net"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

const (
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

type Client interface {
	io.Closer

	MemberId() uuid.UUID
	NewTunnel(memberId uuid.UUID, tunnelId uint64) (Tunnel, error)
	NewListener(tunnelId uint64) (Listener, error)
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
// Examples:
//   c := NewMux(...)
//   s := c.connect(0,0)
//
// Example:
//
type client struct {
	ctx        common.Context
	memberId   uuid.UUID
	dispatcher Dispatcher
	connector  mnet.Connector
	ctrl       circuit.Controller
	closed     chan struct{}
	close      chan struct{}
}

func NewClient(ctx common.Context, factory net.ConnectionFactory, memberId uuid.UUID) {
	// c := &client{
		// ctx:        ctx,
		// memberId:   memberId,
		// dispatcher: NewDispatcher(ctx, memberId),
		// connector:  mnet.NewConnector(net.NewConnector(factory, ctx.Config())),
		// closed:     make(chan struct{})}
}

// func clientRead(c *client) {
// for {
// select {
// case <-c.closed:
// return
// case <-c.connector.Failed():
// return
// case <-c.connector.Closed():
// return
// case p := <-c.connector.Rx():
// }
//
// select {
// case <-c.closed:
// return
// case <-c.connector.Failed():
// return
// case <-c.connector.Closed():
// return
// case c.dispatcher.Tx() <- p:
// }
// }
// }
//
// // func clientControl(c *client) {
// // select {
// // c.closed
// // }
// // }
//
// func (c *client) Close() error {
// panic("not implemented")
// }
//
// func (c *client) MemberId() uuid.UUID {
// return c.memberId
// }
//
// func (c *client) NewTunnel(memberId uuid.UUID, tunnelId uint64) (Tunnel, error) {
// return newDispatchedTunnel(c.dispatcher, wire.NewAddress(memberId, tunnelId))
// }
//
// func (c *client) NewListener(tunnelId uint64) (Listener, error) {
// return newDispatchedListener(c.dispatcher, tunnelId)
// }
//
// func newDispatchedTunnel(dispatcher Dispatcher, addr wire.Address) (Tunnel, error) {
// tunnelSocket, err := dispatcher.NewTunnelSocket(addr)
// if err != nil {
// return nil, err
// }
//
// return NewTunnel(tunnelSocket), nil
// }
//
// func newDispatchedListener(dispatcher Dispatcher, id uint64) (Listener, error) {
// listenerSocket, err := dispatcher.NewListenerSocket(id)
// if err != nil {
// return nil, err
// }
//
// return newListener(listenerSocket), nil
// }

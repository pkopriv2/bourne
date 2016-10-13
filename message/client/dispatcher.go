package client

import (
	"fmt"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/message/core"
	"github.com/pkopriv2/bourne/message/wire"
	uuid "github.com/satori/go.uuid"
)

type Dispatcher interface {
	MemberId() uuid.UUID

	Rx() <-chan wire.Packet
	Tx() chan<- wire.Packet
	Return() <-chan wire.Packet

	Close() error
	Fail(error)

	NewListenerSocket(id uint64) (ListenerSocket, error)
	NewTunnelSocket(remote wire.Address) (TunnelSocket, error)
}

type TunnelSocket interface {
	core.StandardSocket

	Route() wire.Route
	Context() common.Context
}

type ListenerSocket interface {
	TunnelSocket
	NewTunnel(wire.Address) (TunnelSocket, error)
}

func DispatchRouter(p wire.Packet) interface{} {
	return p.Route().Reverse()
}

type dispatcher struct {
	mux  core.Multiplexer
	uuid uuid.UUID
	pool *IdPool
}

func NewDispatcher(ctx common.Context, memberId uuid.UUID) Dispatcher {
	return &dispatcher{core.NewMultiplexer(ctx, DispatchRouter), memberId, NewIdPool()}
}

func (d *dispatcher) MemberId() uuid.UUID {
	return d.uuid
}

func (d *dispatcher) Return() <-chan wire.Packet {
	return d.mux.Return()
}

func (d *dispatcher) Rx() <-chan wire.Packet {
	return d.mux.Rx()
}

func (d *dispatcher) Tx() chan<- wire.Packet {
	return d.mux.Tx()
}

func (d *dispatcher) Close() error {
	return d.mux.Close()
}

func (d *dispatcher) Fail(e error) {
	d.mux.Fail(e)
}

func (d *dispatcher) NewTunnelSocket(remote wire.Address) (TunnelSocket, error) {
	id, err := d.pool.Take()
	if err != nil {
		return nil, err
	}

	route := wire.NewRemoteRoute(wire.NewAddress(d.uuid, id), remote)

	socket, err := d.mux.AddSocket(route)
	if err != nil {
		return nil, err
	}

	return &tunnelSocket{d, route, socket}, nil
}

func (d *dispatcher) NewListenerSocket(id uint64) (ListenerSocket, error) {
	if id > 255 {
		return nil, fmt.Errorf("Invalid tunnel id [%v].  Must be in the range [0,255]", id)
	}

	route := wire.NewLocalRoute(wire.NewAddress(d.uuid, id))

	socket, err := d.mux.AddSocket(route)
	if err != nil {
		return nil, err
	}

	return &listenerSocket{d, route, socket}, nil
}

type listenerSocket struct {
	dispatcher *dispatcher
	route      wire.Route
	raw        core.StandardSocket
}

func (l *listenerSocket) Context() common.Context {
	return l.dispatcher.mux.Context()
}

func (l *listenerSocket) Route() wire.Route {
	return l.route
}

func (l *listenerSocket) NewTunnel(remote wire.Address) (TunnelSocket, error) {
	return l.dispatcher.NewTunnelSocket(remote)
}

func (l *listenerSocket) Closed() <-chan struct{} {
	return l.raw.Closed()
}

func (l *listenerSocket) Failed() <-chan struct{} {
	return l.raw.Failed()
}

func (l *listenerSocket) Failure() error {
	return l.raw.Failure()
}

func (l *listenerSocket) Done() {
	l.raw.Done()
}

func (l *listenerSocket) Rx() <-chan wire.Packet {
	return l.raw.Rx()
}

func (l *listenerSocket) Tx() chan<- wire.Packet {
	return l.raw.Tx()
}

type tunnelSocket struct {
	dispatcher *dispatcher
	route      wire.Route
	raw        core.StandardSocket
}

func (t *tunnelSocket) Context() common.Context {
	return t.dispatcher.mux.Context()
}

func (t *tunnelSocket) Route() wire.Route {
	return t.route
}

func (t *tunnelSocket) Closed() <-chan struct{} {
	return t.raw.Closed()
}

func (t *tunnelSocket) Failed() <-chan struct{} {
	return t.raw.Failed()
}

func (t *tunnelSocket) Failure() error {
	return t.raw.Failure()
}

func (t *tunnelSocket) Done() {
	t.dispatcher.pool.Return(t.Route().Src().ChannelId())
	t.raw.Done()
}

func (t *tunnelSocket) Rx() <-chan wire.Packet {
	return t.raw.Rx()
}

func (t *tunnelSocket) Tx() chan<- wire.Packet {
	return t.raw.Tx()
}

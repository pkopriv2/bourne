package client

import (
	"fmt"
	"sync"

	"github.com/pkopriv2/bourne/msg/core"
	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
	uuid "github.com/satori/go.uuid"
)

const (
	ClientRoutingErrorCode = 201
)

var (
	NewRoutingError = wire.NewProtocolErrorFamily(ClientRoutingErrorCode)
)

const (
	confClientTunnelRx   = "bourne.msg.client.tunnel.rx.size"
	confClientListenerRx = "bourne.msg.client.tunnel.rx.size"
)

const (
	defaultClientTunnelRx   = 1024
	defaultClientListenerRx = 1024
)

type Dispatcher interface {
	SpawnListener(wire.Address) (ListenerSubscription, error)
	SpawnTunnel(uuid.UUID, wire.Address) (TunnelSubscription, error)
	Close()
}

type ListenerSubscription interface {
	core.Transmitter
	core.Receiver
	core.Controller
	utils.Configured

	Address() wire.Address
	SpawnTunnel(remote wire.Address) (TunnelSubscription, error)
	Unsubscribe()
}

type TunnelSubscription interface {
	core.Transmitter
	core.Receiver
	core.Controller
	utils.Configured

	Route() wire.Route
	Unsubscribe()
}

type listenerSubscription struct {
	addr   wire.Address
	conf utils.Config
	parent *dispatcher
	rx     chan wire.Packet
	err    chan error
}

func newListenerSubscription(addr wire.Address, p *dispatcher) *listenerSubscription {
	rx := make(chan wire.Packet, p.config.OptionalInt(confClientListenerRx, defaultClientListenerRx))
	return &listenerSubscription{
		conf: p.config,
		addr:   addr,
		parent: p,
		rx:     rx,
		err:    make(chan error, 1)}
}

func (l *listenerSubscription) RxTx() chan<- wire.Packet {
	return l.rx
}

func (l *listenerSubscription) Address() wire.Address {
	return l.addr
}

func (l *listenerSubscription) SpawnTunnel(remote wire.Address) (TunnelSubscription, error) {
	return l.parent.SpawnTunnel(l.addr.MemberId(), remote)
}

func (l *listenerSubscription) Unsubscribe() {
	l.parent.UnsubscribeListener(l.addr)
}

func (l *listenerSubscription) Tx() chan<- wire.Packet {
	return l.parent.connector.Tx()
}

func (l *listenerSubscription) Rx() <-chan wire.Packet {
	return l.rx
}

func (l *listenerSubscription) Err() <-chan error {
	return l.err
}

func (l *listenerSubscription) Shutdown() <-chan struct{} {
	return l.parent.close
}

type tunnelSubscription struct {
	route  wire.Route
	conf utils.Config
	parent *dispatcher
	rx     chan wire.Packet
	err    chan error
}

func newTunnelSubscription(route wire.Route, p *dispatcher) *tunnelSubscription {
	rx := make(chan wire.Packet, p.config.OptionalInt(confClientTunnelRx, defaultClientTunnelRx))
	return &tunnelSubscription{
		config: p.config,
		route:   route,
		parent: p,
		rx:     rx,
		err:    make(chan error, 1)}
}

func (t *tunnelSubscription) Route() wire.Route {
	return t.route
}

func (t *tunnelSubscription) Unsubscribe() {
	t.parent.UnsubscribeTunnel(t.route)
}

func (l *tunnelSubscription) RxTx() chan<- wire.Packet {
	return l.rx
}

func (t *tunnelSubscription) Tx() chan<- wire.Packet {
	return t.parent.connector.Tx()
}

func (t *tunnelSubscription) Rx() <-chan wire.Packet {
	return t.rx
}

func (t *tunnelSubscription) Shutdown() <-chan struct{} {
	return t.parent.close
}

func ErrorMsg(e error) func(core.ServiceTransmitter) {
	return func(r core.ServiceTransmitter) {
		r.Err() <- e
	}
}

type dispatcher struct {
	listeners *utils.ConcurrentMap
	tunnels   *utils.ConcurrentMap
	pool      *IdPool
	connector core.Connector
	config    utils.Config
	close     chan struct{}
	wait      sync.WaitGroup
}

func NewDispatcher(conf utils.Config, connector core.Connector) Dispatcher {
	d := &dispatcher{
		listeners: utils.NewConcurrentMap(),
		tunnels:   utils.NewConcurrentMap(),
		pool:      NewIdPool(),
		connector: connector,
		close:     make(chan struct{})}
	go Route(d)
	return d
}

func (d *dispatcher) SpawnListener(a wire.Address) (ListenerSubscription, error) {
	s := newListenerSubscription(a, d)
	d.listeners.Put(a, s)
	return s, nil
}

func (d *dispatcher) SpawnTunnel(memberId uuid.UUID, remote wire.Address) (TunnelSubscription, error) {
	tunnelId, err := d.pool.Take()
	if err != nil {
		return nil, err
	}

	s := newTunnelSubscription(wire.NewRemoteRoute(wire.NewAddress(memberId, tunnelId), remote), d)
	d.tunnels.Put(s.route, s)
	return s, nil
}

func (d *dispatcher) UnsubscribeListener(k wire.Address) {
	d.listeners.Remove(k)
}

func (d *dispatcher) UnsubscribeTunnel(k wire.Route) {
	d.tunnels.Remove(k)
}

func (d *dispatcher) Close() {
	close(d.close)
}

func (d *dispatcher) broadcastError(fn func(core.ServiceTransmitter)) {
	for _, v := range d.listeners.All() {
		fn(v.(core.ServiceTransmitter))
	}

	for _, v := range d.tunnels.All() {
		fn(v.(core.ServiceTransmitter))
	}
}

func Route(d *dispatcher) {
	var p wire.Packet
	var dest core.ReceiverServiceTransmitter
	for {
		// too much duplication!
		select {
		case <-d.close:
			return
		case e := <-d.connector.Err():
			d.broadcastError(ErrorMsg(e))
			return
		case p = <-d.connector.Rx():
		}

		// see if there is a listener
		dest = d.listeners.Get(p.Route().Dst()).(core.ReceiverServiceTransmitter)
		if dest == nil {
			dest = d.tunnels.Get(p.Route().Reverse()).(core.ReceiverServiceTransmitter)
		}

		if dest == nil {
			err := p.Return().SetError(
				NewRoutingError(fmt.Sprintf("No such route: %v", p.Route()))).Build()

			select {
			case <-d.close:
				return
			case e := <-d.connector.Err():
				d.broadcastError(ErrorMsg(e))
				return
			case d.connector.Tx() <- err:
			}
		}

		select {
		case <-d.close:
			return
		case dest.RxTx() <- p: // clients are designed to NOT block when receiving
		}
	}
}

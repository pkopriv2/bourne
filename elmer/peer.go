package elmer

import (
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/net"
)

// a peer simply binds a network service with the core store machine.
type peer struct {
	ctx    common.Context
	ctrl   common.Control
	logger common.Logger
	net    net.Network
	self   kayak.Host
	core   *machine
	server net.Server
}

func newPeer(ctx common.Context, self kayak.Host, net net.Network, addr string) (h *peer, err error) {
	ctx = ctx.Sub("Elmer")
	defer func() {
		if err != nil {
			ctx.Control().Fail(err)
		}
	}()

	listener, err := net.Listen(30*time.Second, addr)
	if err != nil {
		return nil, err
	}
	ctx.Control().Defer(func(cause error) {
		listener.Close()
	})

	core, err := newStoreMachine(ctx, self, 20)
	if err != nil {
		return nil, err
	}
	ctx.Control().Defer(func(cause error) {
		core.Close()
	})

	server, err := newServer(ctx, listener, core, 50)
	if err != nil {
		return nil, err
	}
	ctx.Control().Defer(func(cause error) {
		server.Close()
	})

	p := &peer{
		ctx:    ctx,
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
		net:    net,
		core:   core,
		server: server,
	}

	return p, p.start(listener.Addr().String())
}

func (p *peer) start(addr string) error {
	// remove peer from roster on close.
	defer p.ctrl.Defer(func(error) {
		timer := p.ctx.Timer(5 * time.Minute)
		defer timer.Close()

		p.core.UpdateRoster(timer.Closed(), func(cur []string) []string {
			return delPeer(cur, addr)
		})
	})

	// add peer to roster (clients can now discover this peer)
	timer := p.ctx.Timer(5 * time.Minute)
	defer timer.Close()
	return p.core.UpdateRoster(timer.Closed(), func(cur []string) []string {
		return addPeer(cur, addr)
	})
}

func (p *peer) Close() error {
	return p.ctrl.Close()
}

func (p *peer) Store() (Store, error) {
	return newStoreClient(p.ctx, p.net, 30*time.Second, 5*time.Minute, p.self.Roster()), nil
}

func (p *peer) Shutdown() error {
	return p.ctrl.Close()
}

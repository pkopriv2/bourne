package elmer

import (
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/net"
)

type peer struct {
	ctx    common.Context
	ctrl   common.Control
	logger common.Logger
	net    net.Network
	self   kayak.Host
	core   *indexer
	server net.Server
	roster *roster
	pool   common.ObjectPool
	addr   string
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

	addr = listener.Addr().String()

	core, err := newIndexer(ctx, self, 20)
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

	pool := common.NewObjectPool(ctx.Control(), 10,
		newStaticClusterPool(ctx, net, 30*time.Second, []string{addr}))
	ctx.Control().Defer(func(cause error) {
		pool.Close()
	})

	p := &peer{
		ctx:    ctx,
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
		net:    net,
		core:   core,
		server: server,
		roster: newRoster(core),
		pool:   pool,
		addr:   addr,
	}

	return p, p.start(addr)
}

func (p *peer) start(addr string) error {
	// remove peer from roster on close.
	defer p.ctrl.Defer(func(error) {
		timer := p.ctx.Timer(5 * time.Minute)
		defer timer.Close()
		p.roster.Del(timer.Closed(), addr)
	})

	// add peer to roster (clients can now discover this peer)
	timer := p.ctx.Timer(5 * time.Minute)
	defer timer.Close()
	return p.roster.Add(timer.Closed(), addr)
}

func (p *peer) Catalog() (Catalog, error) {
	return &catalogClient{p.ctrl.Sub(), p.pool}, nil
}

func (p *peer) Shutdown() error {
	panic("not implemented")
}

func (p *peer) Close() error {
	return p.ctrl.Close()
}

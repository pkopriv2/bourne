package elmer

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/net"
)

func getAddr(self kayak.Host, listener net.Listener) (string, error) {
	rawAddr, _, err := net.SplitAddr(self.Addr())
	if err != nil {
		return "", errors.WithStack(err)
	}

	_, localPort, err := net.SplitAddr(listener.Addr().String())
	if err != nil {
		return "", errors.WithStack(err)
	}

	return net.NewAddr(rawAddr, localPort), nil
}

type peer struct {
	ctx    common.Context
	ctrl   common.Control
	logger common.Logger
	net    net.Network
	self   kayak.Host
	core   *indexer
	roster *roster
	server net.Server
	pool   common.ObjectPool
	addr   string
}

func newPeer(ctx common.Context, self kayak.Host, addr string, opts *Options) (h *peer, err error) {
	defer func() {
		if err != nil {
			ctx.Control().Fail(err)
		}
	}()

	listener, err := opts.Net.Listen(opts.ConnTimeout, addr)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ctx.Control().Defer(func(cause error) {
		listener.Close()
	})

	addr, err = getAddr(self, listener)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ctx = ctx.Sub("Elmer(%v)", addr)

	core, err := newIndexer(ctx, self, 100)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ctx.Control().Defer(func(cause error) {
		core.Close()
	})

	roster := newRoster(core)

	server, err := newServer(ctx, listener, core, roster, opts.ServerWorkers, opts.ConnTimeout)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ctx.Control().Defer(func(cause error) {
		server.Close()
	})

	pool := common.NewObjectPool(ctx.Control(), opts.ConnPool,
		newStaticClusterPool(ctx, opts.Net, opts.ConnTimeout, []string{addr}))
	ctx.Control().Defer(func(cause error) {
		pool.Close()
	})

	p := &peer{
		ctx:    ctx,
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
		net:    opts.Net,
		core:   core,
		self:   self,
		server: server,
		roster: roster,
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

	_, err := p.roster.Add(timer.Closed(), addr)
	return err
}

func (p *peer) Shutdown() error {
	panic("not implemented")
}

func (p *peer) Close() error {
	return p.ctrl.Close()
}

func (p *peer) Roster(cancel <-chan struct{}) ([]string, error) {
	return p.roster.Get(cancel)
}

func (p *peer) Addr() string {
	return p.addr
}

func (p *peer) Root() (Store, error) {
	return newStoreClient(p.ctx, p.pool, emptyPath), nil
}

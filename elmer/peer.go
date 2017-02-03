package elmer

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/net"
)

// a peer simply binds a network service with the core store machine.
type peer struct {
	ctx    common.Context
	ctrl   common.Control
	logger common.Logger
	self   kayak.Peer
	core   *machine
	server net.Server
}

func newPeer(ctx common.Context, listener net.Listener, self kayak.Peer) (h *peer, err error) {
	ctx = ctx.Sub("Elmer")
	defer func() {
		if err != nil {
			ctx.Control().Fail(err)
		}
	}()

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
	return &peer{
		ctx:    ctx,
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
		core:   core,
		server: server,
	}, nil
}

func (h *peer) Close() error {
	return h.ctrl.Close()
}

func (p *peer) Get(cancel <-chan struct{}, key []byte) (Item, bool, error) {
	return p.core.Read(cancel, getRpc{Key: key})
}

func (p *peer) Put(cancel <-chan struct{}, key []byte, val []byte, prev int) (Item, bool, error) {
	return p.core.Swap(cancel, swapRpc{Key: key, Val: val, Prev: prev})
}

func (p *peer) Del(cancel <-chan struct{}, key []byte, prev int) (bool, error) {
	_, ok, err := p.core.Swap(cancel, swapRpc{Key: key, Prev: prev})
	return ok, err
}

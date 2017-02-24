package kayak

import (
	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// FIXME: Need to discover self address from remote address.

// a host simply binds a network service with the core log machine.
type host struct {
	ctx    common.Context
	ctrl   common.Control
	logger common.Logger
	server net.Server
	core   *replica
	sync   *syncer
	pool   common.ObjectPool // T: *rpcClient
}

func newHost(ctx common.Context, net net.Network, store LogStore, db *bolt.DB, addr string) (h *host, err error) {
	ctx = ctx.Sub("Kayak")
	defer func() {
		if err != nil {
			ctx.Control().Fail(err)
		}
	}()

	listener, err := net.Listen(ctx.Config().OptionalDuration(Config.ConnectionTimeout, Config.ConnectionTimeoutDefault), addr)
	if err != nil {
		return nil, err
	}
	ctx.Control().Defer(func(cause error) {
		listener.Close()
	})

	core, err := newReplica(ctx, net, store, db, listener.Addr().String())
	if err != nil {
		return nil, err
	}
	ctx.Control().Defer(func(cause error) {
		core.Close()
	})

	server, err := newServer(ctx, core, listener, ctx.Config().OptionalInt(Config.ServerWorkers, Config.ServerWorkdersDefault))
	if err != nil {
		return nil, err
	}
	ctx.Control().Defer(func(cause error) {
		server.Close()
	})

	pool := newLeaderPool(core, 10)
	ctx.Control().Defer(func(cause error) {
		pool.Close()
	})

	sync := newSyncer(pool)
	ctx.Control().Defer(func(cause error) {
		sync.Close()
	})

	return &host{
		ctx:    ctx,
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
		core:   core,
		server: server,
		pool:   pool,
		sync:   sync,
	}, nil
}

func (h *host) Fail(e error) error {
	h.ctrl.Fail(e)
	return h.ctrl.Failure()
}

func (h *host) Close() error {
	return h.Fail(h.Leave())
}

func (h *host) Id() uuid.UUID {
	return h.core.Id
}

func (h *host) Context() common.Context {
	return h.core.Ctx
}

func (h *host) Addr() string {
	return h.core.Self.Addr
}

func (h *host) Self() Peer {
	return h.core.Self
}

func (h *host) Peers() []Peer {
	return h.core.Others()
}

func (h *host) Cluster() []Peer {
	return h.core.Cluster()
}

func (h *host) Roster() []Peer {
	return h.core.Cluster()
}

func (h *host) Sync() (Sync, error) {
	return h.sync, nil
}

func (h *host) Log() (Log, error) {
	return newLogClient(h.core, h.pool), nil
}

func (h *host) Addrs() []string {
	addrs := make([]string, 0, 8)
	for _, p := range h.core.Cluster() {
		addrs = append(addrs, p.Addr)
	}
	return addrs
}

func (h *host) Start() error {
	becomeFollower(h.core)
	return nil
}

func (h *host) Join(addr string) error {
	var err error

	becomeFollower(h.core)
	defer func() {
		if err != nil {
			h.core.ctrl.Fail(err)
			h.ctx.Logger().Error("Error joining: %v", err)
		}
	}()

	for attmpt := 0; attmpt < 3; attmpt++ {
		err = h.tryJoin(addr)
		if err != nil {
			h.ctx.Logger().Error("Attempt(%v): Error joining cluster: %v: %v", addr, attmpt, err)
			continue
		}
		break
	}

	return err
}

func (h *host) Leave() error {
	var err error
	for attmpt := 0; attmpt < 3; attmpt++ {
		err = h.tryLeave()
		if err != nil {
			h.ctx.Logger().Error("Attempt(%v): Error leaving cluster: %v", attmpt, err)
			continue
		}
		break
	}

	h.ctx.Logger().Info("Shutting down: %v", err)
	h.core.ctrl.Fail(err)
	return err
}

func (h *host) tryJoin(addr string) error {
	cl, err := connect(h.core.Ctx, h.core.Network, h.core.ConnTimeout, addr)
	if err != nil {
		return errors.Wrapf(err, "Error connecting to peer [%v]", addr)
	}
	defer cl.Close()

	status, err := cl.Status()
	if err != nil {
		return errors.Wrapf(err, "Error joining cluster [%v]", addr)
	}

	h.core.Term(status.term.Num, nil, nil)
	return cl.UpdateRoster(h.core.Self, true)
}

func (h *host) tryLeave() error {
	peer := h.core.Leader()
	if peer == nil {
		return NoLeaderError
	}

	cl, err := peer.Client(h.core.Ctx, h.core.Network, h.core.ConnTimeout)
	if err != nil {
		return err
	}
	defer cl.Close()
	return cl.UpdateRoster(h.core.Self, false)
}

func hostsCollect(hosts []*host, fn func(h *host) bool) []*host {
	ret := make([]*host, 0, len(hosts))
	for _, h := range hosts {
		if fn(h) {
			ret = append(ret, h)
		}
	}
	return ret
}

func hostsFirst(hosts []*host, fn func(h *host) bool) *host {
	for _, h := range hosts {
		if fn(h) {
			return h
		}
	}
	return nil
}

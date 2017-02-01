package kayak

import (
	"fmt"
	"io"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

func newLeaderPool(self *replica, size int) common.ObjectPool {
	leaderFn := func() (io.Closer, error) {
		var cl *rpcClient
		// var err error

		for cl == nil {
			leader := self.Leader()
			if leader == nil {
				time.Sleep(self.ElectionTimeout)
				continue
			}

			cl, _ = leader.Client(self.Ctx)
		}
		return cl, nil
	}
	return common.NewObjectPool(self.Ctx, fmt.Sprintf("LeaderPool"), leaderFn, size)
}

// a host simply binds a network service with the core log machine.
type host struct {
	ctx    common.Context
	ctrl   common.Control
	logger common.Logger
	server net.Server
	core   *replica
	pool   common.ObjectPool
}

func newHost(ctx common.Context, self string, store LogStore, db *bolt.DB) (h *host, err error) {
	ctx = ctx.Sub("Kayak")
	defer func() {
		if err != nil {
			ctx.Control().Fail(err)
		}
	}()

	core, err := newReplica(ctx, self, store, db)
	if err != nil {
		return nil, err
	}

	pool := newLeaderPool(core, 10)
	ctx.Control().Defer(func(cause error) {
		pool.Close()
	})

	// FIXME: Refactor net.Server to accept addrs instead of ports.
	_, port, err := net.SplitAddr(self)
	if err != nil {
		return nil, err
	}

	server, err := newServer(ctx, ctx.Logger(), port, core)
	if err != nil {
		return nil, err
	}

	ctx.Control().Defer(func(cause error) {
		server.Close()
	})

	h = &host{
		ctx:    ctx,
		core:   core,
		server: server,
		pool:   pool,
	}
	return
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
	cl, err := connect(h.ctx, addr)
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

	cl, err := peer.Client(h.ctx)
	if err != nil {
		return err
	}
	defer cl.Close()
	return cl.UpdateRoster(h.core.Self, false)
}

func (h *host) Close() error {
	return h.ctx.Control().Close()
}

func (h *host) Id() uuid.UUID {
	return h.core.Id
}

func (h *host) Context() common.Context {
	return h.core.Ctx
}

func (h *host) Hostname() string {
	host, _, err := net.SplitAddr(h.Self().Addr)
	if err != nil {
		panic(err)
	}
	return host
}

func (h *host) Roster() []string {
	hostnames := make([]string, 0, 8)
	for _, p := range h.core.Cluster() {
		host, _, err := net.SplitAddr(p.Addr)
		if err != nil {
			panic(err)
		}

		hostnames = append(hostnames, host)
	}
	return hostnames
}


func (h *host) Self() peer {
	return h.core.Self
}

func (h *host) Peers() []peer {
	return h.core.Others()
}

func (h *host) Cluster() []peer {
	return h.core.Cluster()
}

func (h *host) Client() (*rpcClient, error) {
	return h.Self().Client(h.Context())
}

func (h *host) Sync() (Sync, error) {
	return newSyncer(h.pool), nil
}

func (h *host) Log() (Log, error) {
	return newLogClient(h.core, h.pool), nil
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

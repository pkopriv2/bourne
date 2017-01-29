package kayak

import (
	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// a host simply binds a network service with the core log machine.
type host struct {
	ctx    common.Context
	server net.Server
	core   *replica
}

func newHost(ctx common.Context, self string, store LogStore, db *bolt.DB) (h *host, err error) {
	ctx = ctx.Sub("Kayak")

	core, err := newReplica(ctx, self, store, db)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			core.ctrl.Fail(err)
		}
	}()

	// FIXME: Refactor net.Server to accept addrs instead of ports.
	_, port, err := net.SplitAddr(self)
	if err != nil {
		return nil, err
	}

	server, err := newServer(ctx, ctx.Logger(), port, core)
	if err != nil {
		return nil, err
	}

	ctx.Control().OnClose(func(cause error) {
		server.Close()
	})

	h = &host{
		ctx:    ctx,
		core:   core,
		server: server,
	}
	return
}

func (h *host) Start() error {
	becomeFollower(h.core)
	return nil
}

func (h *host) Join(addr string) error {
	cl, err := connect(h.ctx, addr)
	if err != nil {
		return errors.Wrapf(err, "Error while joining cluster [%v]", addr)
	}
	defer cl.Close()

	status, err := cl.Status()
	if err != nil {
		return errors.Wrapf(err, "Unable to retrieve status from [%v]", addr)
	}

	h.core.Term(status.term.Num, nil, nil)
	becomeFollower(h.core)

	if err := cl.UpdateRoster(h.core.Self, true); err != nil {
		h.core.ctrl.Fail(err)
		return err
	} else {
		return nil
	}
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

func (h *host) Self() peer {
	return h.core.Self
}

func (h *host) Peers() []peer {
	return h.core.Others()
}

func (h *host) Log() *eventLog {
	return h.core.Log
}

func (h *host) Append(e Event) (LogItem, error) {
	return LogItem{}, nil
	// return h.core.Append(e, )
}

func (h *host) Client() (*rpcClient, error) {
	return h.Self().Client(h.Context())
}

func (h *host) Listen(from int, buf int) (Listener, error) {
	return h.core.Listen(from, buf)
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

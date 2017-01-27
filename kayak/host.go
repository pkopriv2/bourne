package kayak

import (
	"github.com/boltdb/bolt"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// a host simply binds a network service with the core log machine.
type host struct {
	logger common.Logger
	server net.Server
	core   *replica
	closed chan struct{}
	closer chan struct{}
}

func newHost(ctx common.Context, self string, store LogStore, db *bolt.DB) (h *host, err error) {
	root := ctx.Logger().Fmt("Kayak")

	core, err := newReplica(ctx, root, self, store, db)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			core.Close()
		}
	}()

	_, port, err := net.SplitAddr(self)
	if err != nil {
		return nil, err
	}

	server, err := newServer(ctx, root, port, core)
	if err != nil {
		return nil, err
	}

	h = &host{
		core:   core,
		server: server,
		logger: root,
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1),
	}
	return
}

func (h *host) becomeFollower() {
	becomeFollower(h.core)
}

func (h *host) becomeInitiate() {
	becomeInitiate(h.core)
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

func (h *host) Close() error {
	select {
	case <-h.closed:
		return ClosedError
	case h.closer <- struct{}{}:
	}

	h.logger.Info("Closing")

	var err error
	err = common.Or(err, h.server.Close())
	err = common.Or(err, h.core.Close())
	close(h.closed)
	return err
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

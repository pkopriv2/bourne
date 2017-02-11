package convoy

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// A host manages a single replica instance.  Most importantly, if an
// instance is deemed unhealthy and shuts down, the host will attempt
// create a new replica instance.
type host struct {

	// the central context.  Shared amongst all objects within the host graph.
	ctx common.Context

	// the root logger.
	logger common.Logger

	// lifecycle control
	ctrl common.Control

	// the host id
	id uuid.UUID

	// the network abstraction
	net net.Network

	// the
	server net.Server

	// the local store.
	db *database

	// request channels
	chs *replica

	// the local server address
	addr string

	// constantly pushes the current value until the replica has been replaced.
	inst chan *replicaEpoch
}

func newHost(ctx common.Context, db *database, network net.Network, addr string, peers []string) (*host, error) {
	// FIXME: Allow sub contexts without formatting
	ctx = ctx.Sub("")

	id, err := db.Log().Id()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	list, err := network.Listen(30*time.Second, addr)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ctx.Control().Defer(func(error) {
		list.Close()
	})

	chs := newReplica(ctx, network)
	server, err := newServer(ctx, chs, list, 30)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ctx.Control().Defer(func(error) {
		server.Close()
	})

	h := &host{
		ctx:    ctx,
		logger: ctx.Logger(),
		ctrl:   ctx.Control(),
		id:     id,
		net:    network,
		server: server,
		db:     db,
		chs:    chs,
		addr:   list.Addr().String(),
		inst:   make(chan *replicaEpoch),
	}

	if err := h.start(peers); err != nil {
		return nil, err
	}

	return h, nil
}

func (h *host) start(peers []string) error {
	// FIXME: get version from changelog!!!
	cur, err := h.epoch(0, peers)
	if err != nil {
		return errors.WithStack(err)
	}

	go func() {
		defer h.logger.Info("Manager shutting down")
		for {
			select {
			case <-h.ctrl.Closed():
				return
			case h.inst <- cur:
				continue
			case <-cur.Ctrl.Closed():
				err := cur.Ctrl.Failure()
				if err != FailedError {
					h.ctrl.Fail(err)
					return
				}

				for i := 0; ; i++ {
					h.logger.Info("Attempt [%v] to rejoin cluster.", i)
					if tmp, err := h.epoch(cur.Self.version+1, peers); err == nil {
						cur = tmp
						break
					}
				}

				h.logger.Info("Successfully rejoined cluster")
			}
		}
	}()
	return nil
}

func (h *host) Id() uuid.UUID {
	return h.id
}

func (h *host) Close() error {
	return h.Leave()
}

func (h *host) Shutdown() error {
	h.logger.Info("Shutting down forcefully")
	return h.ctrl.Close()
}

func (h *host) Leave() error {
	h.logger.Info("Shutting down gracefully")
	h.ctrl.Fail(h.chs.Leave(h.ctx.Timer(30 * time.Second)))
	return h.ctrl.Failure()
}

func (h *host) Self() (Member, error) {
	return h.chs.Self(h.ctx.Timer(30 * time.Second))
}

func (h *host) Directory() (Directory, error) {
	return &localDir{h.chs}, nil
}

func (h *host) self(ver int, peers []string) (member, error) {
	if peers == nil {
		host, port, err := net.SplitAddr(h.addr)
		if err != nil {
			return member{}, errors.WithStack(err)
		}

		return newMember(h.id, host, port, ver), nil
	}

	try := func(peer string) (member, error) {
		cl, err := connectMember(h.ctx, h.net, 30*time.Second, peer)
		if err != nil {
			return member{}, err
		}
		defer cl.Close()

		host, _, err := net.SplitAddr(cl.Raw.Local().String())
		if err != nil {
			return member{}, err
		}

		_, port, err := net.SplitAddr(h.addr)
		if err != nil {
			return member{}, errors.WithStack(err)
		}

		return newMember(h.id, host, port, ver), nil
	}

	var mem member
	var err error
	for _, peer := range peers {
		mem, err = try(peer)
		if err == nil {
			return mem, nil
		}
	}

	return mem, err
}

func (h *host) epoch(ver int, peers []string) (*replicaEpoch, error) {
	self, err := h.self(ver, peers)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	cur, err := newEpoch(h.chs, h.net, h.db, self)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if peers != nil && len(peers) > 0 {
		if err := cur.Join(peers); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	return cur, nil
}

// The host db simply manages access to the underlying local store.
// Mostly it prevents consumers from erroneously disconnecting the
// local database.
type localDb struct {
	h *host
}

func (d *localDb) Close() error {
	return nil
}

func (d *localDb) Get(key string) (bool, Item, error) {
	return d.h.db.Get(key)
}

func (d *localDb) Put(key string, val string, expected int) (bool, Item, error) {
	return d.h.db.Put(key, val, expected)
}

func (d *localDb) Del(key string, expected int) (bool, Item, error) {
	return d.h.db.Del(key, expected)
}

type localDir struct {
	chs *replica
}

func (h *localDir) Close() error {
	return nil
}

func (h *localDir) Joins() (Listener, error) {
	if h.chs.ctrl.IsClosed() {
		return nil, errors.WithStack(common.ClosedError)
	}
	return h.chs.Joins(), nil
}

func (h *localDir) Evictions() (Listener, error) {
	if h.chs.ctrl.IsClosed() {
		return nil, errors.WithStack(common.ClosedError)
	}
	return h.chs.Evictions(), nil
}

func (h *localDir) Failures() (Listener, error) {
	if h.chs.ctrl.IsClosed() {
		return nil, errors.WithStack(common.ClosedError)
	}
	return h.chs.Failures(), nil
}

func (h *localDir) Get(cancel <-chan struct{}, id uuid.UUID) (Member, error) {
	for !common.IsCanceled(cancel) {
		raw, err := h.chs.DirView(cancel, func(dir *directory) interface{} {
			if m, ok := dir.Get(id); ok {
				return m
			} else {
				return nil
			}
		})
		if err != nil || raw == nil {
			continue
		}
		return raw.(Member), nil
	}
	return nil, errors.WithStack(common.CanceledError)
}

func (h *localDir) All(cancel <-chan struct{}) ([]Member, error) {
	for !common.IsCanceled(cancel) {
		raw, err := h.chs.DirView(cancel, func(dir *directory) interface{} {
			return toMembers(dir.AllActive())
		})
		if err != nil {
			continue
		}
		return raw.([]Member), nil
	}
	return nil, errors.WithStack(common.CanceledError)
}

func (h *localDir) Evict(cancel <-chan struct{}, m Member) error {
	return h.chs.Evict(cancel, m)
}

func (h *localDir) Fail(cancel <-chan struct{}, m Member) error {
	return h.chs.Fail(cancel, m)
}

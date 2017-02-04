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
	chs *rpcChannels

	// the local server address
	addr string

	// constantly pushes the current value until the replica has been replaced.
	inst chan *replica
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

	chs := newRpcChannels(ctx)
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
		inst:   make(chan *replica),
	}

	if err := h.start(peers); err != nil {
		return nil, err
	}

	return h, nil
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
	h.ctrl.Fail(h.chs.Leave(h.ctx.Timer(30*time.Second)))
	return h.ctrl.Failure()
}

func (h *host) Self() (Member, error) {
	rep, err := h.instance()
	if err != nil {
		return nil, err
	}

	return rep.Self, nil
}

func (h *host) Directory() (Directory, error) {
	return &hostDir{h.chs}, nil
}

func (h *host) instance() (r *replica, err error) {
	select {
	case <-h.ctrl.Closed():
		return nil, common.Or(ClosedError, h.ctrl.Failure())
	case r = <-h.inst:
	}
	return
}

func (h *host) shutdown(inst *replica, reason error) error {
	inst.Ctrl.Fail(reason)
	return inst.Ctrl.Failure()
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

		host, port, err := net.SplitAddr(cl.Raw.Local().String())
		if err != nil {
			return member{}, err
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

func (h *host) initReplica(ver int, peers []string) (*replica, error) {
	self, err := h.self(ver, peers)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	cur, err := newReplica(h.chs, h.net, h.db, self)
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

func (h *host) start(peers []string) error {
	// FIXME: get version from changelog!!!
	cur, err := h.initReplica(0, peers)
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
					if tmp, err := h.initReplica(cur.Self.version + 1, peers); err == nil {
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

// The host db simply manages access to the underlying local store.
// Mostly it prevents consumers from erroneously disconnecting the
// local database.
type hostDb struct {
	h *host
}

func (d *hostDb) Close() error {
	return nil
}

func (d *hostDb) Get(key string) (bool, Item, error) {
	return d.h.db.Get(key)
}

func (d *hostDb) Put(key string, val string, expected int) (bool, Item, error) {
	return d.h.db.Put(key, val, expected)
}

func (d *hostDb) Del(key string, expected int) (bool, Item, error) {
	return d.h.db.Del(key, expected)
}

type hostDir struct {
	chs *rpcChannels
}

func (h *hostDir) Close() error {
	panic("not implemented")
}

func (h *hostDir) Get(cancel <-chan struct{}, id uuid.UUID) (Member, error) {
	panic("not implemented")
}

func (h *hostDir) All(cancel <-chan struct{}) ([]Member, error) {
	for ! common.IsCanceled(cancel) {
		dir, err := h.chs.Directory(cancel)
		if err != nil {
			continue
		}

		return toMembers(dir.AllActive()), nil
	}
	return nil, errors.WithStack(common.CanceledError)
}

func (h *hostDir) Evict(cancel <-chan struct{}, id uuid.UUID) error {
	panic("not implemented")
}

func (h *hostDir) Fail(cancel <-chan struct{}, id uuid.UUID) error {
	panic("not implemented")
}

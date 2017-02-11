package convoy

import (
	"fmt"
	"strings"
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
	iface *replicaIface

	// the local server address
	addr string

	// constantly pushes the current value until the replica has been replaced.
	inst chan *replicaEpoch
}

func newHost(ctx common.Context, db *database, network net.Network, addr string, peers []string) (*host, error) {
	// FIXME: Allow sub contexts without formatting

	id, err := db.Log().Id()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ctx = ctx.Sub("Host(%v)", id.String()[:8])
	ctx.Logger().Info("Starting")

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
		iface:  chs,
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
		defer h.logger.Info("Shutting down")
		for {
			select {
			case <-h.ctrl.Closed():
				return
			case h.inst <- cur:
				continue
			case <-cur.Ctrl.Closed():
				err := cur.Ctrl.Failure()
				h.logger.Info("Epoch [%v] died", cur.Self.version)

				if err := errors.Cause(err); err != FailedError {
					h.ctrl.Fail(err)
					return
				}

				for i := 1; ; i++ {
					if h.ctrl.IsClosed() {
						return
					}

					h.logger.Info("Attempt [%v] to rejoin cluster.", i)
					if tmp, err := h.epoch(cur.Self.version+i, membersAddrs(cur.Dir.AllHealthy())); err == nil {
						cur = tmp
						break
					}
				}

				h.logger.Info("Successfully rejoined cluster: %v", cur.Self)
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
	timer := h.ctx.Timer(30 * time.Second)
	defer timer.Close()
	h.ctrl.Fail(h.iface.Leave(timer.Closed()))
	return h.ctrl.Failure()
}

func (h *host) Self() (Member, error) {
	timer := h.ctx.Timer(30 * time.Second)
	defer timer.Close()
	return h.iface.Self(timer.Closed())
}

func (h *host) Directory() (Directory, error) {
	return &localDir{h.iface}, nil
}

func (h *host) Store() (Store, error) {
	return &localDb{h.db}, nil
}

func (h *host) epoch(ver int, peers []string) (*replicaEpoch, error) {
	return initEpoch(h.iface, h.net, h.db, h.id, ver, h.addr, peers)
}

// The host db simply manages access to the underlying local store.
// Mostly it prevents consumers from erroneously disconnecting the
// local database.
type localDb struct {
	db *database
}

func (d *localDb) Close() error {
	return nil
}

func (d *localDb) Get(cancel <-chan struct{}, key string) (bool, Item, error) {
	return d.db.Get(key)
}

func (d *localDb) Put(cancel <-chan struct{}, key string, val string, expected int) (bool, Item, error) {
	return d.db.Put(key, val, expected)
}

func (d *localDb) Del(cancel <-chan struct{}, key string, expected int) (bool, Item, error) {
	return d.db.Del(key, expected)
}

type localDir struct {
	iface *replicaIface
}

func (h *localDir) Close() error {
	return nil
}

func (h *localDir) Joins() (Listener, error) {
	if h.iface.ctrl.IsClosed() {
		return nil, errors.WithStack(common.ClosedError)
	}
	return h.iface.Joins(), nil
}

func (h *localDir) Evictions() (Listener, error) {
	if h.iface.ctrl.IsClosed() {
		return nil, errors.WithStack(common.ClosedError)
	}
	return h.iface.Evictions(), nil
}

func (h *localDir) Failures() (Listener, error) {
	if h.iface.ctrl.IsClosed() {
		return nil, errors.WithStack(common.ClosedError)
	}
	return h.iface.Failures(), nil
}

func (h *localDir) Get(cancel <-chan struct{}, id uuid.UUID) (Member, error) {
	for !common.IsCanceled(cancel) {
		raw, err := h.iface.DirView(cancel, func(dir *directory) interface{} {
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
		raw, err := h.iface.DirView(cancel, func(dir *directory) interface{} {
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
	return h.iface.Evict(cancel, m)
}

func (h *localDir) Fail(cancel <-chan struct{}, m Member) error {
	return h.iface.Fail(cancel, m)
}

func (h *localDir) String() string {
	timer := h.iface.ctx.Timer(30 * time.Second)
	defer timer.Close()

	all, err := h.All(timer.Closed())
	if err != nil {
		return "Error(Unable to print dir)"
	}

	strs := make([]string, 0, len(all))
	for _, e := range all {
		strs = append(strs, fmt.Sprintf("%v", e))
	}
	return strings.Join(strs, "\n")
}

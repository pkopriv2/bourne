package convoy

import (
	"sync"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// A host manages a single replica instance.  Most importantly, if an
// instance is deemed unhealthy and shutdown,
type host struct {

	// the central context.  Shared amongst all objects with the host graph.
	ctx common.Context

	// the root logger.
	logger common.Logger

	// the host id
	id uuid.UUID

	// the published database.  Updates to this are merged and distributed within host.
	db *database

	// the local hostname of the host.  Other members use THIS address to contact the member.
	hostname string

	// the port that the replica will be hosted on.
	port int

	// the number of join attempts
	attempts int

	// the method for retrieving connections to the peer.  returns nil if master.
	// invoked on each new instance of the replica.
	peer func() (*client, error)

	// the failure.  set if closed returns a value.
	failure error

	// constantly pushes the current value until the replica
	// has been replaced.
	inst chan *replica

	// the wait group.  will return once all child routines have returned.
	wait sync.WaitGroup

	// returns a value when the o
	closed chan struct{}

	// the lock that does the failure assignment.
	closer chan struct{}
}

func newSeedHost(ctx common.Context, db *database, hostname string, port int) (*host, error) {
	return newHost(ctx, db, hostname, port, "") // TODO: figure out how to reliably address localhost
}

func newHost(ctx common.Context, db *database, hostname string, port int, peer string) (*host, error) {
	id, err := db.Log().Id()
	if err != nil {
		return nil, err
	}

	h := &host{
		ctx:      ctx,
		logger:   ctx.Logger().Fmt("Host(%v,%v)", hostname, port),
		id:       id,
		db:       db,
		hostname: hostname,
		port:     port,
		attempts: ctx.Config().OptionalInt(Config.JoinAttempts, defaultJoinAttemptsCount),
		inst:     make(chan *replica),
		closed:   make(chan struct{}),
		closer:   make(chan struct{}, 1),
		peer: func() (*client, error) {
			if peer == "" {
				return nil, nil
			} else {
				return connectMember(ctx, ctx.Logger().Fmt("PeerFn"), peer)
			}
		},
	}

	if err := h.start(); err != nil {
		return nil, err
	}

	return h, nil
}

func (h *host) Shutdown() error {
	h.logger.Info("Shutting down forcefully")

	rep, err := h.instance()
	if err != nil {
		return err
	}

	return h.shutdown(rep, nil)
}

func (h *host) Close() error {
	h.logger.Info("Shutting down gracefully")
	return h.Leave()
}

func (h *host) Leave() error {
	rep, err := h.instance()
	if err != nil {
		return err
	}

	return h.shutdown(rep, rep.Leave())
}

func (h *host) Id() uuid.UUID {
	return h.id
}

func (h *host) Version() int {
	rep, err := h.instance()
	if err != nil {
		return 0
	}

	return rep.Self.version
}

func (h *host) Connect(port int) (net.Connection, error) {
	rep, err := h.instance()
	if err != nil {
		return nil, err
	}

	return rep.Self.Connect(port)
}

func (h *host) Store() Store {
	return h.db
}

func (h *host) Directory() Directory {
	return &hostDir{h}
}

func (h *host) instance() (r *replica, err error) {
	select {
	case <-h.closed:
		return nil, common.Or(h.failure, ClosedError)
	case r = <-h.inst:
	}
	return
}

func (h *host) shutdown(inst *replica, reason error) error {
	select {
	case <-h.closed:
		return ClosedError
	case h.closer <- struct{}{}:
	}

	// shutdown the db.
	h.db.Close()

	// cleanup the current instance
	if inst != nil {
		inst.Close()
	}

	h.failure = reason
	close(h.closed)
	h.wait.Wait()
	return nil
}

func (h *host) ensureOpen() error {
	select {
	case <-h.closed:
		return common.Or(h.failure, ClosedError)
	default:
		return nil
	}
}

func (h *host) newReplica() (*replica, error) {
	cl, err := h.peer()
	if err != nil {
		return nil, err
	}

	if cl == nil {
		return newSeedReplica(h.ctx, h.db, h.hostname, h.port)
	}
	defer cl.Close()
	return newReplica(h.ctx, h.db, h.hostname, h.port, cl)
}

func (h *host) start() error {
	cur, err := h.newReplica()
	if err != nil {
		return err
	}

	h.wait.Add(1)
	go func() {
		defer h.wait.Done()
		defer h.logger.Info("Manager shutting down")
		for {
			select {
			case <-h.closed:
				return
			case h.inst <- cur:
				continue
			case <-cur.Closed:

				// if it shutdown due to irrecoverable failure, just bail out
				if cur.Failure != FailedError {
					h.shutdown(cur, cur.Failure)
					return
				}

				var err error
				var tmp *replica
				for i := 0; i < h.attempts; i++ {
					h.logger.Info("Attempt [%v] to rejoin cluster.", i)
					tmp, err = h.newReplica()
					if err != nil {
						continue
					}

					cur = tmp
					break
				}

				if err != nil {
					h.logger.Info("Unable to rejoin cluster")
					h.shutdown(nil, err)
					return
				}

				h.logger.Info("Successfully rejoined cluster")
			}
		}
	}()
	return nil
}

// directory wrapper...
type hostDir struct {
	h *host
}

func (d *hostDir) Get(id uuid.UUID) (Member, error) {
	for {
		replica, err := d.h.instance()
		if err != nil {
			return nil, err
		}

		m, ok := replica.Dir.Get(id)
		if ok {
			return m, nil
		}
	}
}

func (d *hostDir) All() ([]Member, error) {
	for {
		replica, err := d.h.instance()
		if err != nil {
			return nil, err
		}

		return hostInternalMemberToMember(replica.Dir.AllActive()), nil
	}
}

func (d *hostDir) Evict(m Member) error {
	for {
		replica, err := d.h.instance()
		if err != nil {
			return err
		}

		err = replica.Dir.Evict(m)
		if err != ClosedError {
			return err
		}
	}
}

func (d *hostDir) Fail(m Member) error {
	for {
		replica, err := d.h.instance()
		if err != nil {
			return err
		}

		err = replica.Dir.Fail(m)
		if err != ClosedError {
			return err
		}
	}
}

func (d *hostDir) Search(filter func(uuid.UUID, string, string) bool) ([]Member, error) {
	for {
		replica, err := d.h.instance()
		if err != nil {
			return nil, err
		}

		return hostInternalMemberToMember(replica.Dir.Search(filter)), nil
	}
}

func (d *hostDir) First(filter func(uuid.UUID, string, string) bool) (Member, error) {
	for {
		replica, err := d.h.instance()
		if err != nil {
			return nil, err
		}

		m, ok := replica.Dir.First(filter)
		if ok {
			return m, nil
		} else {
			return nil, nil
		}
	}
}

func hostInternalMemberToMember(arr []member) []Member {
	ret := make([]Member, 0, len(arr))
	for _, m := range arr {
		ret = append(ret, m)
	}

	return ret
}

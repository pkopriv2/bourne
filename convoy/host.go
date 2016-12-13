package convoy

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

var (
	instanceEvicted     = errors.New("Replica evicted from host")
	instanceClosedError = errors.New("Intance closed")
)

// A host manages a single replica instance.  Most importantly, if an
// instance is deemed unhealthy and shutdown,
type host struct {
	// the central context.  Shared amongst all objects with the host graph.
	ctx common.Context

	// the root logger.
	logger common.Logger

	// the published database.  Updates to this are merged and distributed within host.
	db *database

	// the local hostname of the host.  Other members use THIS address to contact the member.
	hostname string

	// the port that the replica will be hosted on.
	port int

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

func newMasterHost(ctx common.Context, db *database, hostname string, port int) (*host, error) {
	return newMemberHost(ctx, db, hostname, port, "") // TODO: figure out how to reliably address localhost
}

func newMemberHost(ctx common.Context, db *database, hostname string, port int, peer string) (*host, error) {
	h := &host{
		ctx:      ctx,
		logger:   ctx.Logger(),
		db:       db,
		hostname: hostname,
		port:     port,
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

func (h *host) newReplica() (*replica, error) {
	cl, err := h.peer()
	if err != nil {
		return nil, err
	}

	if cl == nil {
		return newMasterReplica(h.ctx, h.db, h.hostname, h.port)
	}
	defer cl.Close()
	return newMemberReplica(h.ctx, h.db, h.hostname, h.port, cl)
}

func (h *host) instance() (*replica, error) {
	select {
	case <-h.closed:
		return nil, instanceClosedError
	case r := <-h.inst:
		return r, nil
	}
}

func (h *host) start() error {

	// start 'er up.
	cur, err := h.newReplica()
	if err != nil {
		return err
	}

	h.wait.Add(1)
	go func() {
		defer h.wait.Done()
		for {
			select {
			case <-h.closed:
				return
			case h.inst <- cur:
				continue
			case <-cur.Closed:

				// if it naturally shutdown, just bail out
				if cur.Failure != replicaFailureError {
					h.shutdown(cur.Failure)
					return
				}

				for i := 0; i < 3; i++ {
					cur, err = h.newReplica()
					if err == nil {
						break
					}
				}

				if err != nil {
					h.shutdown(err)
					return
				}
			}
		}
	}()
	return nil
}

func (h *host) shutdown(reason error) error {
	select {
	case <-h.closed:
		return instanceClosedError
	case h.closer <- struct{}{}:
	}

	h.failure = reason
	close(h.closed)
	h.wait.Wait()

	<-h.closer
	return nil
}

func (h *host) Close() error {
	select {
	case <-h.closed:
		return instanceClosedError
	default:
	}

	return h.shutdown(nil)
}

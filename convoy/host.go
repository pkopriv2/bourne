package convoy

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

var (
	instanceEvicted     = errors.New("HOST:EVICTED")
	instanceClosedError = errors.New("HOST:CLOSED")
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

func newSeedHost(ctx common.Context, db *database, hostname string, port int) (*host, error) {
	return newHost(ctx, db, hostname, port, "") // TODO: figure out how to reliably address localhost
}

func newHost(ctx common.Context, db *database, hostname string, port int, peer string) (*host, error) {
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

				// if it shutdown due to irrecoverable failure, just bail out
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

func (h *host) Connect(port int) (net.Connection, error) {
	rep, err := h.instance()
	if err != nil {
		return nil, err
	}

	return rep.Self.Connect(port)
}

func (h *host) Store() (Store, error) {
	select {
	case <-h.closed:
		return nil, instanceClosedError
	default:
	}

	return &hostDb{h}, nil
}

func (h *host) Directory() (Directory, error) {
	select {
	case <-h.closed:
		return nil, instanceClosedError
	default:
	}

	return &hostDir{h}, nil
}

// The host db simply manages access to the underlying local store.
// It is able to shield consumers from connection changes in the host
type hostDb struct {
	h *host
}

func (d *hostDb) Close() error {
	return nil
}

func (d *hostDb) Get(key string) (*Item, error) {
	for {
		replica, err := d.h.instance()
		if err != nil {
			return nil, err
		}

		item, err := replica.Db.Get(key)
		if err == databaseClosedError {
			continue
		}

		return item, nil
	}
}

func (d *hostDb) Put(key string, val string, expected int) (bool, Item, error) {
	for {
		replica, err := d.h.instance()
		if err != nil {
			return false, Item{}, err
		}

		ok, item, err := replica.Db.Put(key, val, expected)
		if err == databaseClosedError {
			continue
		}

		return ok, item, err
	}
}

func (d *hostDb) Del(key string, expected int) (bool, Item, error) {
	for {
		replica, err := d.h.instance()
		if err != nil {
			return false, Item{}, err
		}

		ok, item, err := replica.Db.Del(key, expected)
		if err == databaseClosedError {
			continue
		}

		return ok, item, err
	}
}

// directory wrapper...
type hostDir struct {
	h *host
}

func (d *hostDir) Get(id uuid.UUID) (Member, error) {
	for {
		replica, err := d.h.instance()
		if err != nil {
			continue
		}

		m, ok := replica.Dir.Get(id)
		if ok {
			return m, nil
		} else {
			return nil, nil
		}
	}
}

func (d *hostDir) Evict(m Member) error {
	for {
		replica, err := d.h.instance()
		if err != nil {
			return err
		}

		err = replica.Dir.Evict(m)
		if err != dirClosedError {
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
		if err != dirClosedError {
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

		return hostmemberToMember(replica.Dir.Search(filter)), nil
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

func hostmemberToMember(arr []member) []Member {
	ret := make([]Member, 0, len(arr))
	for _, m := range arr {
		ret = append(ret, m)
	}

	return ret
}

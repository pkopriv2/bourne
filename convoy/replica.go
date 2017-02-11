package convoy

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

type replica struct {
	ctx            common.Context
	ctrl           common.Control
	net            net.Network
	self           chan *common.Request
	joins          chan uuid.UUID
	evictions      chan uuid.UUID
	failures       chan uuid.UUID
	dirReadOnly    chan *common.Request
	dirReadWrite   chan *common.Request
	dissemPushPull chan *common.Request
	leave          chan *common.Request
}

func newReplica(ctx common.Context, net net.Network) *replica {
	return &replica{
		ctx,
		ctx.Control(),
		net,
		make(chan *common.Request),
		make(chan uuid.UUID),
		make(chan uuid.UUID),
		make(chan uuid.UUID),
		make(chan *common.Request),
		make(chan *common.Request),
		make(chan *common.Request),
		make(chan *common.Request),
	}
}

func (r *replica) Joins() *listener {
	return newListener(r.ctx, r.joins)
}

func (r *replica) Evictions() *listener {
	return newListener(r.ctx, r.evictions)
}

func (r *replica) Failures() *listener {
	return newListener(r.ctx, r.failures)
}

func (r *replica) DirView(cancel <-chan struct{}, fn func(dir *directory) interface{}) (interface{}, error) {
	return r.sendRequest(r.dirReadOnly, cancel, fn)
}

func (r *replica) DirUpdate(cancel <-chan struct{}, fn func(dir *directory) (interface{}, error)) (interface{}, error) {
	return r.sendRequest(r.dirReadWrite, cancel, fn)
}

func (r *replica) Self(cancel <-chan struct{}) (Member, error) {
	raw, err := r.sendRequest(r.self, cancel, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return raw.(Member), nil
}

func (r *replica) Evict(cancel <-chan struct{}, m Member) error {
	_, err := r.DirUpdate(cancel, func(dir *directory) (interface{}, error) {
		return nil, dir.Evict(m)
	})
	if err != nil {
		return errors.WithStack(err)
	} else {
		return nil
	}
}

func (r *replica) Fail(cancel <-chan struct{}, m Member) error {
	_, err := r.DirUpdate(cancel, func(dir *directory) (interface{}, error) {
		return nil, dir.Fail(m)
	})
	if err != nil {
		return errors.WithStack(err)
	} else {
		return nil
	}
}

func (r *replica) DirList(cancel <-chan struct{}) (rpcDirListResponse, error) {
	raw, err := r.DirView(cancel, func(dir *directory) interface{} {
		return dir.Events()
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return rpcDirListResponse(raw.([]event)), nil
}

func (r *replica) DirApply(cancel <-chan struct{}, rpc rpcDirApplyRequest) (rpcDirApplyResponse, error) {
	raw, err := r.DirUpdate(cancel, func(dir *directory) (interface{}, error) {
		return dir.Apply(rpc)
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return rpcDirApplyResponse(raw.([]bool)), nil
}

func (r *replica) DirPushPull(cancel <-chan struct{}, rpc rpcPushPullRequest) (rpcPushPullResponse, error) {
	raw, err := r.sendRequest(r.dissemPushPull, cancel, rpc)
	if err != nil {
		return rpcPushPullResponse{}, errors.WithStack(err)
	}
	return raw.(rpcPushPullResponse), nil
}

func (r *replica) Leave(cancel <-chan struct{}) error {
	_, err := r.sendRequest(r.leave, cancel, nil)
	return err
}

func (r *replica) ProxyPing(cancel <-chan struct{}, req rpcPingProxyRequest) (rpcPingResponse, error) {
	raw, err := r.DirView(cancel, func(dir *directory) interface{} {
		m, ok := dir.Get(uuid.UUID(req))
		if !ok {
			return false
		}

		c, err := m.Client(r.ctx, r.net, 30*time.Second)
		if err != nil || c == nil {
			return false
		}

		defer c.Close()
		err = c.Ping()
		return err == nil
	})
	if err != nil {
		return false, errors.WithStack(err)
	}
	return rpcPingResponse(raw.(bool)), nil
}

func (h *replica) sendRequest(ch chan<- *common.Request, cancel <-chan struct{}, val interface{}) (interface{}, error) {
	req := common.NewRequest(val)
	defer req.Cancel()

	select {
	case <-h.ctrl.Closed():
		return nil, errors.WithStack(common.ClosedError)
	case <-cancel:
		return nil, errors.WithStack(common.CanceledError)
	case ch <- req:
		select {
		case <-h.ctrl.Closed():
			return nil, errors.WithStack(common.ClosedError)
		case r := <-req.Acked():
			return r, nil
		case e := <-req.Failed():
			return nil, e
		case <-cancel:
			return nil, errors.WithStack(common.CanceledError)
		}
	}
}

// A replica represents a live, joined instance of a convoy db. The instance itself is managed by the
// host object.
type replicaEpoch struct {
	*replica

	// the central context.
	Ctx common.Context

	// the root logger.  decorated with self's information.
	Logger common.Logger

	// the replica control
	Ctrl common.Control

	// the member hosted by this instance.
	Self member

	// the network abstraction
	Net net.Network

	// the db hosted by this instance.
	Db *database

	// the central directory - contains the local view of all replica's dbs
	Dir *directory

	// the disseminator.  Responsible for pushing and pulling data
	Dissem *disseminator

	// a flag indicating whether this instance is currently leaving.
	leaving concurrent.AtomicBool

	// the primary request work pool.  Using basic threading this time.
	Pool common.WorkPool
}

// Initializes and returns a generic replica instance.
func newEpoch(chs *replica, net net.Network, db *database, self member) (*replicaEpoch, error) {
	ctx := chs.ctx.Sub("%v", self)

	var err error
	defer func() {
		if err != nil {
			ctx.Control().Fail(err)
		}
	}()

	dir, err := replicaInitDir(ctx, db, self)
	if err != nil {
		return nil, err
	}

	diss, err := replicaInitDissem(ctx, net, self, dir)
	if err != nil {
		return nil, err
	}

	r := &replicaEpoch{
		replica: chs,
		Self:    self,
		Ctx:     ctx,
		Net:     net,
		Ctrl:    ctx.Control(),
		Logger:  ctx.Logger(),
		Dir:     dir,
		Db:      db,
		Dissem:  diss,
		Pool:    common.NewWorkPool(ctx.Control(), 50),
	}

	return r, r.start()
}

func (r *replicaEpoch) start() error {
	joins := r.Dir.Joins()
	go func() {
		for j := range joins {
			select {
			case <-r.ctrl.Closed():
				return
			case r.replica.joins <- j.Id:
			}
		}
	}()

	evictions := r.Dir.Evictions()
	go func() {
		for e := range evictions {
			r.Logger.Debug("Member evicted [%v,%v]", e.Id, e.Version)
			if e.Id == r.Self.id && e.Version == r.Self.version && !r.leaving.Get() {
				r.Logger.Info("Self evicted. Shutting down.")
				r.Leave()
				return
			}

			select {
			case <-r.ctrl.Closed():
				return
			case r.replica.evictions <- e.Id:
			}
		}
	}()

	failures := r.Dir.Failures()
	go func() {
		for f := range failures {
			r.Logger.Debug("Member failed [%v,%v]", f.Id, f.Version)
			if f.Id == r.Self.id && f.Version == r.Self.version {
				r.Logger.Error("Self Failed. Shutting down.")
				r.Ctrl.Fail(FailedError)
				return
			}

			select {
			case <-r.ctrl.Closed():
				return
			case r.replica.failures <- f.Id:
			}
		}
	}()

	go func() {
		defer r.Ctrl.Close()
		for !r.Ctrl.IsClosed() {
			select {
			case <-r.Ctrl.Closed():
				return
			case req := <-r.replica.leave:
				req.Fail(r.Leave())
				return
			case req := <-r.replica.self:
				req.Ack(r.Self)
				return
			case req := <-r.replica.dirReadOnly:
				r.handleDirReadOnly(req)
			case req := <-r.replica.dirReadWrite:
				r.handleDirReadWrite(req)
			case req := <-r.replica.dissemPushPull:
				r.handlePushPull(req)
			}
		}
	}()
	return nil
}

func (r *replicaEpoch) handleDirReadOnly(req *common.Request) {
	err := r.Pool.SubmitOrCancel(req.Canceled(), func() {
		fn := req.Body().(func(*directory) interface{})
		req.Ack(fn(r.Dir))
	})
	if err != nil {
		req.Fail(err)
	}
}

func (r *replicaEpoch) handleDirReadWrite(req *common.Request) {
	err := r.Pool.SubmitOrCancel(req.Canceled(), func() {
		fn := req.Body().(func(*directory) (interface{}, error))
		req.Return(fn(r.Dir))
	})
	if err != nil {
		req.Fail(err)
	}
}

func (r *replicaEpoch) handlePushPull(req *common.Request) {
	err := r.Pool.SubmitOrCancel(req.Canceled(), func() {
		rpc := req.Body().(rpcPushPullRequest)

		var unHealthy bool
		r.Dir.Core.View(func(v *view) {
			m, ok := v.Roster[rpc.id]
			h, _ := v.Health[rpc.id]
			unHealthy = ok && m.Version == rpc.version && m.Active && !h.Healthy
		})

		if unHealthy {
			r.Logger.Error("Unhealthy member detected [%v]", rpc.id)
			req.Fail(FailedError)
			return
		}

		ret, err := r.Dir.Apply(rpc.events)
		if err != nil {
			req.Fail(errors.WithStack(err))
			return
		}

		req.Ack(rpcPushPullResponse{ret, r.Dissem.events.Pop(1024)})
	})
	if err != nil {
		req.Fail(err)
	}
}

func (r *replicaEpoch) Id() uuid.UUID {
	return r.Self.id
}

func (r *replicaEpoch) Close() error {
	return r.Ctrl.Close()
}

func (r *replicaEpoch) Join(peers []string) error {
	try := func(addr string) error {
		peer, err := connectMember(r.ctx, r.Net, 30*time.Second, addr)
		if err != nil {
			return errors.WithStack(err)
		}

		// Register self with the peer.  (Should result in realtime updates being delivered to self.)
		_, err = peer.DirApply(r.Dir.Events())
		if err != nil {
			return errors.Wrap(err, "Error registering self with peer")
		}

		// Download the peer's directory.
		events, err := peer.DirList()
		if err != nil {
			return errors.Wrap(err, "Error retrieving directory list from peer")
		}

		r.Dir.Apply(events)
		return nil
	}

	var err error
	for _, addr := range peers {
		if err = try(addr); err == nil {
			return nil
		}
	}
	return err
}

func (r *replicaEpoch) Leave() error {
	if r.Ctrl.IsClosed() {
		return common.Or(ClosedError, r.Ctrl.Failure())
	}

	if !r.leaving.Swap(false, true) {
		return errors.New("Already leaving")
	}

	return r.leaveAndDrain()
}

// guaranteed to be called only once.
func (r *replicaEpoch) leaveAndDrain() error {
	r.Logger.Info("Leaving")

	if err := r.Dir.Evict(r.Self); err != nil {
		r.Logger.Error("Error evicting self [%v]", err)
		return errors.Wrap(err, "Error evicting self")
	}

	timer := r.Ctx.Timer(30 * time.Second)
	for size := r.Dissem.events.data.Size(); size > 0; size = r.Dissem.events.data.Size() {
		select {
		default:
			r.Logger.Info("Remaining items: %v", size)
			time.Sleep(1 * time.Second)
		case <-r.Ctrl.Closed():
			return errors.WithStack(ClosedError)
		case <-timer:
			return nil
		}
	}

	return nil
}

// Helper functions

// Returns a newly initialized directory that is populated with the given db and member
// and is indexing realtime changes to the db.
func replicaInitDir(ctx common.Context, db *database, self member) (*directory, error) {

	dir := newDirectory(ctx)
	ctx.Control().Defer(func(error) {
		dir.Close()
	})

	// start indexing realtime changes.
	// !!! MUST HAPPEN PRIOR TO BACKFILLING !!!
	listener, err := db.Log().Listen()
	if err != nil {
		return nil, err
	}
	ctx.Control().Defer(func(error) {
		listener.Close()
	})
	dirIndexEvents(
		changeStreamToEventStream(
			self, listener.ch), dir)

	// Grab all the changes from the database
	chgs, err := db.Log().All()
	if err != nil {
		return nil, err
	}

	dir.Add(self)
	dir.Apply(changesToEvents(self, chgs))
	return dir, nil
}

// Returns a newly initialized disseminator.
func replicaInitDissem(ctx common.Context, net net.Network, self member, dir *directory) (*disseminator, error) {
	dissem, err := newDisseminator(ctx, net, self, dir)
	if err != nil {
		return nil, errors.Wrap(err, "Error constructing disseminator")
	}
	ctx.Control().Defer(func(error) {
		dissem.Close()
	})

	// Start disseminating realtime changes.
	dissemEvents(dirListen(dir), dissem)
	return dissem, nil
}

func replicaClient(server net.Server) (*rpcClient, error) {
	raw, err := server.Client(net.Json)
	if err != nil {
		return nil, err
	}

	return &rpcClient{raw}, nil
}

type listener struct {
	ctrl common.Control
	in   <-chan uuid.UUID
	out  chan uuid.UUID
}

func newListener(ctx common.Context, in <-chan uuid.UUID) *listener {
	l := &listener{
		ctrl: ctx.Control().Sub(),
		in:   in,
		out:  make(chan uuid.UUID),
	}
	l.start()
	return l
}

func (l *listener) start() {
	go func() {
		defer l.Close()

		for {
			var id uuid.UUID
			select {
			case <-l.ctrl.Closed():
				return
			case id = <-l.in:
			}

			select {
			case <-l.ctrl.Closed():
				return
			case l.out <- id:
			}
		}
	}()
}

func (l *listener) Data() <-chan uuid.UUID {
	return l.out
}

func (l *listener) Ctrl() common.Control {
	return l.ctrl
}

func (l *listener) Close() error {
	return l.ctrl.Close()
}

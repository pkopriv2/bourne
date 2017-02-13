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
	ctx             common.Context
	ctrl            common.Control
	net             net.Network
	self            chan *common.Request
	dirReadOnly     chan *common.Request
	dirReadWrite    chan *common.Request
	dissemPushPull  chan *common.Request
	leave           chan *common.Request
	shutdown        chan *common.Request
	rosterListeners concurrent.Set
	healthListeners concurrent.Set
}

func newReplica(ctx common.Context, net net.Network) *replica {
	ctx = ctx.Sub("Replica")
	return &replica{
		ctx,
		ctx.Control(),
		net,
		make(chan *common.Request),
		make(chan *common.Request),
		make(chan *common.Request),
		make(chan *common.Request),
		make(chan *common.Request),
		make(chan *common.Request),
		concurrent.NewSet(),
		concurrent.NewSet(),
	}
}

func (r *replica) ListenRoster() *rosterListener {
	ret := newRosterListener(r.ctrl)
	ret.Ctrl().Defer(func(error) {
		r.rosterListeners.Remove(ret)
	})
	r.rosterListeners.Add(ret)
	return ret
}

func (r *replica) ListenHealth() *healthListener {
	ret := newHealthListener(r.ctrl)
	ret.Ctrl().Defer(func(error) {
		r.healthListeners.Remove(ret)
	})
	r.healthListeners.Add(ret)
	return ret
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

func (r *replica) Dump(cancel <-chan struct{}) (string, error) {
	raw, err := r.DirView(cancel, func(dir *directory) interface{} {
		return dir.String()
	})
	if err != nil {
		return "", errors.WithStack(err)
	}
	return raw.(string), nil
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

func (r *replica) Shutdown() error {
	_, err := r.sendRequest(r.shutdown, nil, nil)
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

// listener impl
type rosterListener struct {
	ctrl common.Control
	out  chan Membership
}

func newRosterListener(ctrl common.Control) *rosterListener {
	return &rosterListener{
		ctrl: ctrl.Sub(),
		out:  make(chan Membership),
	}
}

func (l *rosterListener) Send() chan<- Membership {
	return l.out
}

func (l *rosterListener) Data() <-chan Membership {
	return l.out
}

func (l *rosterListener) Closed() <-chan struct{} {
	return l.ctrl.Closed()
}

func (l *rosterListener) Ctrl() common.Control {
	return l.ctrl
}

func (l *rosterListener) Close() error {
	return l.ctrl.Close()
}

type healthListener struct {
	ctrl common.Control
	out  chan Health
}

func newHealthListener(ctrl common.Control) *healthListener {
	return &healthListener{
		ctrl: ctrl.Sub(),
		out:  make(chan Health),
	}
}

func (l *healthListener) Send() chan<- Health {
	return l.out
}

func (l *healthListener) Data() <-chan Health {
	return l.out
}

func (l *healthListener) Closed() <-chan struct{} {
	return l.ctrl.Closed()
}

func (l *healthListener) Ctrl() common.Control {
	return l.ctrl
}

func (l *healthListener) Close() error {
	return l.ctrl.Close()
}

// An epoch implements a single birth-death cycle of a replica.
type epoch struct {
	Iface *replica

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

func initEpoch(iface *replica, net net.Network, db *database, id uuid.UUID, ver int, addr string, peers []string) (*epoch, error) {
	self, err := replicaInitSelf(iface.ctx, net, id, ver, addr, peers)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	cur, err := newEpoch(iface, net, db, self)
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

// Initializes and returns a generic replica instance.
func newEpoch(iface *replica, net net.Network, db *database, self member) (*epoch, error) {
	ctx := iface.ctx.Sub("Epoch(%v)", self.version)
	ctx.Logger().Info("Starting epoch")

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

	r := &epoch{
		Iface:  iface,
		Self:   self,
		Ctx:    ctx,
		Net:    net,
		Ctrl:   ctx.Control(),
		Logger: ctx.Logger(),
		Dir:    dir,
		Db:     db,
		Dissem: diss,
		Pool:   common.NewWorkPool(ctx.Control(), 50),
	}

	return r, r.start()
}

func (r *epoch) start() error {

	// Start eviction thread.  This is responsible for shutting down the epoch in the
	// event that it has been evicted.
	go func() {
		evictions := r.Dir.Evictions()
		for e := range evictions {
			r.Logger.Debug("Member evicted [%v,%v]", e.Id.String()[:8], e.Version)
			if e.Id == r.Self.id && e.Version == r.Self.version && !r.leaving.Get() {
				r.Logger.Info("Self evicted. Shutting down.")
				r.Ctrl.Fail(r.Leave())
				return
			}
		}
	}()

	// Start failure thread.  This is responsible for shutting down the epoch in the
	// event that it has been failed (ie unhealthy).
	go func() {
		failures := r.Dir.Failures()
		for f := range failures {
			r.Logger.Debug("Member failed [%v,%v]", f.Id.String()[:8], f.Version)
			if f.Id == r.Self.id && f.Version == r.Self.version {
				r.Logger.Error("Self Failed. Shutting down.")
				r.Ctrl.Fail(FailedError)
				return
			}
		}
	}()

	// start the listener threads
	go func() {
		updates := r.Dir.ListenRoster()
		for u := range updates {
			for _, raw := range r.Iface.rosterListeners.All() {
				l := raw.(*rosterListener)
				select {
				case <-r.Ctrl.Closed():
					return
				case <-l.Closed():
				case l.Send() <- u:
				}
			}
		}
	}()

	go func() {
		updates := r.Dir.ListenHealth()
		for u := range updates {
			for _, raw := range r.Iface.healthListeners.All() {
				l := raw.(*healthListener)
				select {
				case <-r.Ctrl.Closed():
					return
				case <-l.Closed():
				case l.Send() <- u:
				}
			}
		}
	}()

	go func() {
		defer r.Ctrl.Close()
		for !r.Ctrl.IsClosed() {
			select {
			case <-r.Ctrl.Closed():
				return
			case req := <-r.Iface.shutdown:
				r.handleShutdown(req)
				return
			case req := <-r.Iface.leave:
				r.handleLeave(req)
				return
			case req := <-r.Iface.self:
				req.Ack(r.Self)
			case req := <-r.Iface.dirReadOnly:
				r.handleDirReadOnly(req)
			case req := <-r.Iface.dirReadWrite:
				r.handleDirReadWrite(req)
			case req := <-r.Iface.dissemPushPull:
				r.handlePushPull(req)
			}
		}
	}()
	return nil
}

func (r *epoch) handleShutdown(req *common.Request) {
	req.Ack(nil)
}

func (r *epoch) handleLeave(req *common.Request) {
	err := r.Leave()
	r.Ctrl.Fail(err)
	req.Fail(err)
}

func (r *epoch) handleDirReadOnly(req *common.Request) {
	err := r.Pool.SubmitOrCancel(req.Canceled(), func() {
		fn := req.Body().(func(*directory) interface{})
		req.Ack(fn(r.Dir))
	})
	if err != nil {
		req.Fail(err)
	}
}

func (r *epoch) handleDirReadWrite(req *common.Request) {
	err := r.Pool.SubmitOrCancel(req.Canceled(), func() {
		fn := req.Body().(func(*directory) (interface{}, error))
		req.Return(fn(r.Dir))
	})
	if err != nil {
		req.Fail(err)
	}
}

func (r *epoch) handlePushPull(req *common.Request) {
	err := r.Pool.SubmitOrCancel(req.Canceled(), func() {
		rpc := req.Body().(rpcPushPullRequest)

		// for _, e := range rpc.events {
		// // r.Logger.Info("Handling push: %v", e)
		// }

		var unHealthy bool
		r.Dir.Core.View(func(v *view) {
			m, ok := v.Roster[rpc.id]
			h, _ := v.Health[rpc.id]
			unHealthy = ok && m.Version >= rpc.version && m.Active && !h.Healthy
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

func (r *epoch) Id() uuid.UUID {
	return r.Self.id
}

func (r *epoch) Close() error {
	return r.Ctrl.Close()
}

func (r *epoch) Join(peers []string) error {
	r.Logger.Info("Joining cluster [%v] ...", peers[:common.Min(3, len(peers))])

	try := func(addr string) error {
		peer, err := connectMember(r.Ctx, r.Net, 30*time.Second, addr)
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

func (r *epoch) Leave() error {
	if r.Ctrl.IsClosed() {
		return common.Or(ClosedError, r.Ctrl.Failure())
	}

	if !r.leaving.Swap(false, true) {
		return errors.New("Already leaving")
	}

	return r.leaveAndDrain()
}

// guaranteed to be called only once.
func (r *epoch) leaveAndDrain() error {
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
		case <-timer.Closed():
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
				return nil, errors.WithStack(err)
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
				return nil, errors.WithStack(err)
			}

			return &rpcClient{raw}, nil
		}

		func replicaInitSelf(ctx common.Context, network net.Network, id uuid.UUID, ver int, addr string, peers []string) (member, error) {
			addr, err := replicaInitAddr(ctx, network, addr, peers)
			if err != nil {
				return member{}, errors.WithStack(err)
			}

			host, port, err := net.SplitAddr(addr)
			if err != nil {
				return member{}, errors.WithStack(err)
			}

			return newMember(id, host, port, ver), nil
		}

		func replicaInitAddr(ctx common.Context, network net.Network, addr string, peers []string) (string, error) {
			if peers == nil {
				return addr, nil
			}

			try := func(peer string) (string, error) {
				cl, err := connectMember(ctx, network, 30*time.Second, peer)
				if err != nil {
					return "", err
				}
				defer cl.Close()

				host, _, err := net.SplitAddr(cl.Raw.Local().String())
				if err != nil {
					return "", err
				}

				_, port, err := net.SplitAddr(addr)
				if err != nil {
					return "", errors.WithStack(err)
				}

				return net.NewAddr(host, port), nil
			}

			var err error
			for _, peer := range peers {
				addr, err = try(peer)
				if err == nil {
					return addr, nil
				}
			}

			return addr, err
		}

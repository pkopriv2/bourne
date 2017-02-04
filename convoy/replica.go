package convoy

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// A replica represents a live, joined instance of a convoy db. The instance itself is managed by the
// host object.
type replica struct {
	*rpcChannels

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
func newReplica(chs *rpcChannels, net net.Network, db *database, self member) (*replica, error) {
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

	r := &replica{
		rpcChannels: chs,
		Self:        self,
		Ctx:         ctx,
		Net:         net,
		Ctrl:        ctx.Control(),
		Logger:      ctx.Logger(),
		Dir:         dir,
		Db:          db,
		Dissem:      diss,
		Pool:        common.NewWorkPool(ctx.Control(), 50),
	}

	return r, r.start()
}

func (r *replica) start() error {
	joins := r.Dir.Joins()
	go func() {
		for j := range joins {
			r.Logger.Debug("Member joined [%v,%v]", j.Id.String()[:8], j.Version)
		}
	}()

	evictions := r.Dir.Evictions()
	go func() {
		for e := range evictions {
			r.Logger.Debug("Member evicted [%v,%v]", e.Id, e.Version)
			if e.Id == r.Self.id && e.Version == r.Self.version && !r.leaving.Get() {
				r.Logger.Info("Self evicted. Shutting down.")
				r.Leave()
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
			}
		}
	}()

	go func() {
		defer r.Ctrl.Close()
		for !r.Ctrl.IsClosed() {
			select {
			case <-r.Ctrl.Closed():
				return
			case req := <-r.rpcChannels.leave:
				req.Fail(r.Leave())
				return
			case req := <-r.rpcChannels.healthProxyPing:
				r.handleProxyPing(req)
			case req := <-r.rpcChannels.dir:
				r.handleDirGet(req)
			case req := <-r.rpcChannels.dirList:
				r.handleDirList(req)
			case req := <-r.rpcChannels.dirApply:
				r.handleDirApply(req)
			case req := <-r.rpcChannels.dirPushPull:
				r.handlePushPull(req)
			}
		}
	}()
	return nil
}

func (r *replica) handleDirGet(req *common.Request) {
	req.Ack(r.Dir)
}

func (r *replica) handleProxyPing(req *common.Request) {
	err := r.Pool.SubmitOrCancel(req.Canceled(), func() {
		raw := req.Body().(rpcPingProxyRequest)

		m, ok := r.Dir.Get(uuid.UUID(raw))
		if !ok {
			req.Ack(rpcPingResponse(false))
			return
		}

		c, err := m.Client(r.Ctx, r.Net, 30*time.Second)
		if err != nil || c == nil {
			req.Ack(rpcPingResponse(false))
			return
		}

		defer c.Close()
		err = c.Ping()
		req.Ack(rpcPingResponse(err == nil))
	})
	if err != nil {
		req.Fail(err)
	}
}

func (r *replica) handleDirList(req *common.Request) {
	err := r.Pool.SubmitOrCancel(req.Canceled(), func() {
		req.Ack(rpcDirListResponse(r.Dir.Events()))
	})
	if err != nil {
		req.Fail(err)
	}
}

func (r *replica) handleDirApply(req *common.Request) {
	err := r.Pool.SubmitOrCancel(req.Canceled(), func() {
		flags, err := r.Dir.Apply(req.Body().(rpcDirApplyRequest))
		if err != nil {
			req.Fail(err)
			return
		}

		req.Ack(rpcDirApplyResponse(flags))
	})
	if err != nil {
		req.Fail(err)
	}
}

func (r *replica) handlePushPull(req *common.Request) {
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

func (r *replica) Id() uuid.UUID {
	return r.Self.id
}

func (r *replica) Close() error {
	return r.Ctrl.Close()
}

func (r *replica) Join(peers []string) error {
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

func (r *replica) Leave() error {
	if r.Ctrl.IsClosed() {
		return common.Or(ClosedError, r.Ctrl.Failure())
	}

	if !r.leaving.Swap(false, true) {
		return errors.New("Already leaving")
	}

	return r.leaveAndDrain()
}

// guaranteed to be called only once.
func (r *replica) leaveAndDrain() error {
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

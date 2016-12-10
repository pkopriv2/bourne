package convoy

import (
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

var (
	replicaEvictedError = errors.New("Replica evicted from cluster")
	replicaFailureError = errors.New("Replica failed")
)

// A replica represents a live, possibly joined instance of a convoy db.
// The instance itself is managed by the cluster object.
type replica struct {
	// the central context.
	Ctx common.Context

	// the root logger.  decorated with self's information.
	Logger common.Logger

	// the db hosted by this instance.
	Db Database

	// the member hosted by this instance.
	Self member

	// the central directory - contains the local view of all replica's dbs
	Dir *directory

	// the disseminator.  Responsible for pushing and pulling data
	Dissem *disseminator

	// A simple control channel.  Used to disconnect the changelog.
	disconnect chan<- struct{}

	// the core network server.
	Server net.Server

	// Guaranteed to be set after closed returns values.
	Failure error

	// Channel will be closed when instance is closed.
	Closed chan struct{}

	// closing utils.
	closer    sync.Once
	closeLock sync.Mutex
	leaving   concurrent.AtomicBool
}

func newMasterReplica(ctx common.Context, db Database, hostname string, port int) (r *replica, err error) {
	return initReplica(ctx, db, hostname, port)
}

func newMemberReplica(ctx common.Context, db Database, hostname string, port int, peer *client) (r *replica, err error) {
	r, err = initReplica(ctx, db, hostname, port)
	if err != nil {
		return
	}
	defer common.RunIf(func() { r.shutdown(err) })(err)

	if err = replicaJoin(r, peer); err != nil {
		return nil, err
	}
	return
}

// Initializes and returns a generic replica instance.
func initReplica(ctx common.Context, db Database, host string, port int) (r *replica, err error) {
	var self member
	var dir *directory
	var log chan<- struct{}
	var diss *disseminator
	var server net.Server

	// The replica will be inextricably bound to this exact version of itself.
	self, err = replicaInitSelf(ctx, db, host, port)
	if err != nil {
		return nil, errors.Wrap(err, "Error initializing self")
	}

	// Decorate the root logger with the 'self' instance
	logger := replicaInitLogger(ctx, self)
	logger.Info("Starting replica.")

	dir, log, err = replicaInitDir(ctx, logger, db, self)
	if err != nil {
		return
	}
	defer common.RunIf(func() { dir.Close() })(err)
	defer common.RunIf(func() { close(log) })(err)

	diss, err = replicaInitDissem(ctx, logger, self, dir)
	if err != nil {
		return
	}
	defer common.RunIf(func() { diss.Close() })(err)

	server, err = replicaInitServer(ctx, logger, self, dir, diss, port)
	if err != nil {
		return
	}
	defer common.RunIf(func() { server.Close() })(err)

	r = &replica{
		Self:       self,
		Ctx:        ctx,
		Dir:        dir,
		Db:         db,
		Logger:     logger,
		Dissem:     diss,
		Server:     server,
		disconnect: log,
		Closed:     make(chan struct{})}

	r.Dir.OnEviction(func(id uuid.UUID, ver int) {
		if r.leaving.Get() {
			return
		}

		if id == r.Self.Id {
			r.Logger.Info("Evicted")
			r.Fail(replicaEvictedError)
		}
	})

	r.Dir.OnFailure(func(id uuid.UUID, ver int) {
		if r.leaving.Get() {
			return
		}

		if id == r.Self.Id {
			r.Logger.Info("Failed")
			r.Fail(replicaFailureError)
		}
	})

	return r, nil
}

func (r *replica) Close() error {
	return r.Fail(nil)
}

func (r *replica) Fail(err error) (ret error) {
	return r.Shutdown(func() error { return err; })
}

func (r *replica) Shutdown(fn func() error) (ret error) {
	r.closer.Do(func() { ret = r.shutdown(fn()) })
	return ret
}

func (r *replica) Leave() error {
	r.leaving.Set(true)
	defer r.leaving.Set(false)
	return r.Shutdown(r.leaveAndDrain)
}

func (r *replica) Id() uuid.UUID {
	return r.Self.Id
}

func (r *replica) Client() (*client, error) {
	if err := r.EnsureNotClosed(); err != nil {
		return nil, err
	}

	return replicaClient(r.Server)
}

func (r *replica) EnsureNotClosed() error {
	select {
	case <-r.Closed:
		return common.Or(r.Failure, errors.New("Replica closed"))
	default:
		return nil
	}
}

// guaranteed to be called only once.
func (r *replica) shutdown(err error) (ret error) {
	ret = err

	r.Logger.Info("Shutting down [%v]", err)
	defer common.RunIf(func() { r.Logger.Error("Shutdown error: %v", err) })(err)

	defer close(r.Closed)
	defer common.RunIf(func() { r.Failure = ret })(ret)

	var err1 error
	done1, timeout1 := concurrent.NewBreaker(5*time.Second, func() interface{} {
		err1 = r.Server.Close()
		return nil
	})
	var err2 error
	done2, timeout2 := concurrent.NewBreaker(5*time.Second, func() interface{} {
		err2 = r.Dissem.Close()
		return nil
	})
	var err3 error
	done3, timeout3 := concurrent.NewBreaker(5*time.Second, func() interface{} {
		err3 = r.Dir.Close()
		return nil
	})

	select {
	case <-done1:
		ret = common.Or(ret, err1)
	case <-timeout1:
		ret = common.Or(ret, errors.New("Timeout Closing Server"))
	}

	select {
	case <-done2:
		ret = common.Or(ret, err2)
	case <-timeout2:
		ret = common.Or(ret, errors.New("Timeout Closing Disseminator"))
	}

	select {
	case <-done3:
		ret = common.Or(ret, err3)
	case <-timeout3:
		ret = common.Or(ret, errors.New("Timeout Closing Directory"))
	}

	close(r.disconnect)
	return
}

// guaranteed to be called only once.
func (r *replica) leaveAndDrain() error {
	if err := r.Dir.Evict(r.Self); err != nil {
		r.Logger.Error("Error evicting self [%v]", err)
		return errors.Wrap(err, "Error evicting self")
	}

	done, timeout := concurrent.NewBreaker(10*time.Minute, func() interface{} {
		for r.Dissem.Evts.Data.Size() > 0 {
			time.Sleep(5 * time.Second)
		}
		return nil
	})

	select {
	case <-done:
		return nil
	case <-timeout:
		return errors.New("Timeout while emptying queue")
	}
}

// Helper functions

// Joins the replica to the given peer.
func replicaJoin(self *replica, peer *client) error {
	// Register self with the peer.  (Should result in realtime updates being delivered to self.)
	_, err := peer.DirApply(self.Dir.Events())
	if err != nil {
		return errors.Wrap(err, "Error registering self with peer")
	}

	// Download the peer's directory.
	events, err := peer.DirList()
	if err != nil {
		return errors.Wrap(err, "Error retrieving directory list from peer")
	}

	self.Dir.Apply(events)
	return nil
}

// Returns the member representing "self"
func replicaInitSelf(ctx common.Context, db Database, hostname string, port int) (mem member, err error) {
	var id uuid.UUID
	var seq int
	id, err = db.Log().Id()
	if err != nil {
		return
	}

	seq, err = db.Log().Inc()
	if err != nil {
		return
	}

	mem = newMember(id, hostname, strconv.Itoa(port), seq)
	return
}

// Returns a logger decorated with membership info.
func replicaInitLogger(ctx common.Context, self member) common.Logger {
	return ctx.Logger().Fmt(self.String())
}

// Returns a newly initialized directory that is populated with the given db and member
// and is indexing realtime changes to the db.
func replicaInitDir(ctx common.Context, logger common.Logger, db Database, self member) (*directory, chan<- struct{}, error) {
	dir := newDirectory(ctx, logger)

	// start indexing realtime changes.
	// !!! MUST HAPPEN PRIOR TO BACKFILLING !!!
	chgStream, ctrl := changeLogListen(db.Log())
	dirIndexEvents(
		changeStreamToEventStream(
			self, chgStream), dir)

	// Grab all the changes from the database
	chgs, err := db.Log().All()
	if err != nil {
		return nil, nil, err
	}

	dir.Join(self)
	dir.Apply(changesToEvents(self, chgs))
	return dir, ctrl, nil
}

// Returns a newly initialized disseminator.
func replicaInitDissem(ctx common.Context, logger common.Logger, self member, dir *directory) (*disseminator, error) {
	dissem, err := newDisseminator(ctx, logger, self, dir, 100*time.Millisecond)
	if err != nil {
		return nil, errors.Wrap(err, "Error constructing disseminator")
	}

	// Start disseminating realtime changes.
	dissemEvents(dirListen(dir), dissem)
	return dissem, nil
}

// Returns a newly initialized server.
func replicaInitServer(ctx common.Context, log common.Logger, self member, dir *directory, dissem *disseminator, port int) (net.Server, error) {
	return newServer(ctx, log, self, dir, dissem, port)
}

func replicaClient(server net.Server) (*client, error) {
	raw, err := server.Client()
	if err != nil {
		return nil, err
	}

	return &client{raw}, nil
}

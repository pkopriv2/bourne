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

	// Closing lock
	closer sync.Mutex

	// Channel will be closed when instance is closed.
	Closed chan struct{}
}

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

func newMasterReplica(ctx common.Context, db Database, hostname string, port int) (r *replica, err error) {
	// TODO: FIGURE OUT HOW TO GET IP....BOUNC
	return initReplica(ctx, db, hostname, port)
}

func newMemberReplica(ctx common.Context, db Database, hostname string, port int, peer *client) (r *replica, err error) {
	r, err = initReplica(ctx, db, hostname, port)
	if err != nil {
		return
	}
	defer common.RunIfNotNil(err, func() { r.Shutdown(err) })

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
	self, err = replicaInitSelf(ctx, db, port)
	if err != nil {
		return nil, errors.Wrap(err, "Error initializing self")
	}

	// Decorate the root logger with the 'self' instance
	logger := replicaInitLogger(ctx, self)
	logger.Info("Starting replica.")

	dir, log, err = replicaInitDir(ctx, logger, db, self)
	if err != nil {
		return nil, errors.Wrap(err, "Error initializing directory")
	}
	defer common.RunIfNotNil(err, func() { dir.Close() })
	defer common.RunIfNotNil(err, func() { close(log) })

	diss, err = replicaInitDissem(ctx, logger, self, dir)
	if err != nil {
		return nil, errors.Wrap(err, "Error initializing disseminator")
	}
	defer common.RunIfNotNil(err, func() { diss.Close() })

	server, err = replicaInitServer(ctx, logger, self, dir, diss, port)
	if err != nil {
		return nil, errors.Wrap(err, "Error initializing server")
	}
	defer common.RunIfNotNil(err, func() { server.Close() })

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
		if id == r.Self.Id {
			r.Shutdown(replicaEvictedError)
		}
	})

	r.Dir.OnFailure(func(id uuid.UUID, ver int) {
		if id == r.Self.Id {
			r.Shutdown(replicaFailureError)
		}
	})

	return r, nil
}

func (r *replica) ensureNotClosed() error {
	select {
	case <-r.Closed:
		return r.Failure
	default:
		return nil
	}
}

func (r *replica) Shutdown(err error) error {
	r.closer.Lock()
	defer r.closer.Unlock()
	if err := r.ensureNotClosed(); err != nil {
		return err
	}

	defer func() {
		if err != nil {
			r.Logger.Error("Shutdown error: %v", err)
		}
	}()
	defer func() { r.Failure = err }()
	defer close(r.Closed)

	close(r.disconnect)
	done1, timeout1 := concurrent.NewBreaker(time.Second, func() interface{} {
		return r.Server.Close()
	})
	done2, timeout2 := concurrent.NewBreaker(time.Second, func() interface{} {
		return r.Dissem.Close()
	})
	done3, timeout3 := concurrent.NewBreaker(time.Second, func() interface{} {
		return r.Dir.Close()
	})

	select {
	case e := <-done1:
		if e != nil {
			err = common.ErrOr(err, e.(error))
		}
	case <-timeout1:
		err = common.ErrOr(err, errors.New("Timeout Closing Server"))
	}

	select {
	case e := <-done2:
		if e != nil {
			err = common.ErrOr(err, e.(error))
		}
	case <-timeout2:
		err = common.ErrOr(err, errors.New("Timeout Closing Disseminator"))
	}

	select {
	case e := <-done3:
		if e != nil {
			err = common.ErrOr(err, e.(error))
		}
	case <-timeout3:
		err = common.ErrOr(err, errors.New("Timeout Closing Directory"))
	}

	return err
}

func (r *replica) Close() error {
	return r.Shutdown(nil)
}

func (r *replica) Id() uuid.UUID {
	return r.Self.Id
}

func (r *replica) Client() (*client, error) {
	if err := r.ensureNotClosed(); err != nil {
		return nil, err
	}

	return replicaClient(r.Server)
}

func (r *replica) Leave() error {
	if err := r.ensureNotClosed(); err != nil {
		return err
	}

	r.Logger.Info("Leaving cluster")
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
		return r.Shutdown(nil)
	case <-timeout:
		return r.Shutdown(errors.New("Timeout while leaving"))
	}
}

// Helper functions

// Returns the member representing "self"
func replicaInitSelf(ctx common.Context, db Database, port int) (mem member, err error) {
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

	mem = newMember(id, "localhost", strconv.Itoa(port), seq)
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

package convoy

import (
	"strconv"
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
	// the central context.
	Ctx common.Context

	// the root logger.  decorated with self's information.
	Logger common.Logger

	// the db hosted by this instance.
	Db *database

	// the member hosted by this instance.
	Self member

	// the central directory - contains the local view of all replica's dbs
	Dir *directory

	// the disseminator.  Responsible for pushing and pulling data
	Dissem *disseminator

	// A simple control channel.  Used to disconnect the changelog.
	Changes *changeLogListener

	// the core network server.
	Server net.Server

	// Guaranteed to be set after closed returns values.
	Failure error

	// Channel will be closed when instance is closed.
	Closed chan struct{}

	// closing utils.
	closer  chan struct{}
	leaving concurrent.AtomicBool
}

func newSeedReplica(ctx common.Context, db *database, hostname string, port int) (r *replica, err error) {
	return initReplica(ctx, db, hostname, port)
}

func newReplica(ctx common.Context, db *database, hostname string, port int, peer *client) (r *replica, err error) {
	r, err = initReplica(ctx, db, hostname, port)
	if err != nil {
		return
	}
	defer common.RunIf(func() { r.Close() })(err)

	if err = replicaJoin(r, peer); err != nil {
		return nil, err
	}
	return
}

// Initializes and returns a generic replica instance.
func initReplica(ctx common.Context, db *database, host string, port int) (r *replica, err error) {
	var self member
	var dir *directory
	var chgs *changeLogListener
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

	dir, chgs, err = replicaInitDir(ctx, logger, db, self)
	if err != nil {
		return
	}
	defer common.RunIf(func() { dir.Close() })(err)
	defer common.RunIf(func() { chgs.Close() })(err)

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
		Self:    self,
		Ctx:     ctx,
		Dir:     dir,
		Db:      db,
		Logger:  logger,
		Dissem:  diss,
		Server:  server,
		Changes: chgs,
		Closed:  make(chan struct{}),
		closer:  make(chan struct{}, 1)}

	joins := r.Dir.Joins()
	go func() {
		for j := range joins {
			r.Logger.Debug("Member joined [%v,%v]", j.Id, j.Version)
		}
	}()

	evictions := r.Dir.Evictions()
	go func() {
		for e := range evictions {
			r.Logger.Debug("Member evicted [%v,%v]", e.Id, e.Version)
			if e.Id == r.Self.id && e.Version == r.Self.version && !r.leaving.Get() {
				r.Logger.Info("Self evicted. Shutting down.")
				r.Leave() // We received this message...therefore it was already disseminated.
				// r.shutdown(EvictedError)
			}
		}
	}()

	failures := r.Dir.Failures()
	go func() {
		for f := range failures {
			r.Logger.Debug("Member failed [%v,%v]", f.Id, f.Version)
			if f.Id == r.Self.id && f.Version == r.Self.version {
				r.Logger.Error("Self Failed. Shutting down.")
				r.shutdown(FailedError)
			}
		}
	}()

	return r, nil
}

func (r *replica) ensureOpen() error {
	select {
	case <-r.Closed:
		return common.Or(r.Failure, ClosedError)
	default:
		return nil
	}
}

func (r *replica) Id() uuid.UUID {
	return r.Self.id
}

func (r *replica) Close() error {
	if err := r.ensureOpen(); err != nil {
		return err
	}

	return r.shutdown(nil)
}

func (r *replica) Leave() error {
	if err := r.ensureOpen(); err != nil {
		return err
	}

	if !r.leaving.Swap(false, true) {
		return errors.New("Already leaving")
	}

	return r.shutdown(r.leaveAndDrain())
}

func (r *replica) Client() (*client, error) {
	if err := r.ensureOpen(); err != nil {
		return nil, err
	}

	return replicaClient(r.Server)
}

func (r *replica) shutdown(err error) error {
	select {
	case <-r.Closed:
		return r.Failure
	case r.closer <- struct{}{}:
	}

	defer close(r.Closed)
	defer common.RunIf(func() { r.Logger.Error("shutdown error: %v", err) })(err)
	defer common.RunIf(func() { r.Failure = err })(err)

	var err1 error
	var err2 error
	var err3 error
	done1, timeout1 := concurrent.NewBreaker(5*time.Second, func(){
		err1 = r.Server.Close()
	})
	done2, timeout2 := concurrent.NewBreaker(5*time.Second, func() {
		err2 = r.Dissem.Close()
	})
	done3, timeout3 := concurrent.NewBreaker(5*time.Second, func() {
		err3 = r.Dir.Close()
	})

	select {
	case <-done1:
		err = common.Or(err, err1)
	case e := <-timeout1:
		err = common.Or(err, errors.Wrapf(e, "Timeout Closing Server"))
	}

	select {
	case <-done2:
		err = common.Or(err, err2)
	case e := <-timeout2:
		err = common.Or(err, errors.Wrapf(e, "Timeout Closing Disseminator"))
	}

	select {
	case <-done3:
		err = common.Or(err, err3)
	case e := <-timeout3:
		err = common.Or(err, errors.Wrapf(e, "Timeout Closing Directory"))
	}

	return common.Or(err, r.Changes.Close())
}

// guaranteed to be called only once.
func (r *replica) leaveAndDrain() error {
	r.Logger.Info("Leaving")

	if err := r.Dir.Evict(r.Self); err != nil {
		r.Logger.Error("Error evicting self [%v]", err)
		return errors.Wrap(err, "Error evicting self")
	}

	done, timeout := concurrent.NewBreaker(10*time.Minute, func() {
		for size := r.Dissem.events.data.Size(); size > 0; size = r.Dissem.events.data.Size() {
			select {
			case <-r.Closed:
				return
			default:
			}

			r.Logger.Info("Remaining items: %v", size)
			time.Sleep(1 * time.Second)
		}
	})

	select {
	case <-done:
		return nil
	case e := <-timeout:
		return errors.Wrapf(e, "Timeout while emptying queue.")
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
func replicaInitSelf(ctx common.Context, db *database, hostname string, port int) (member, error) {
	id, err := db.Log().Id()
	if err != nil {
		return member{}, nil
	}

	seq, err := db.Log().Inc()
	if err != nil {
		return member{}, nil
	}

	return newMember(id, hostname, strconv.Itoa(port), seq), nil
}

// Returns a logger decorated with membership info.
func replicaInitLogger(ctx common.Context, self member) common.Logger {
	return ctx.Logger().Fmt(self.String())
}

// Returns a newly initialized directory that is populated with the given db and member
// and is indexing realtime changes to the db.
func replicaInitDir(ctx common.Context, logger common.Logger, db *database, self member) (dir *directory, cl *changeLogListener, err error) {
	dir = newDirectory(ctx, logger)
	defer common.RunIf(func() { dir.Close() })(err)

	// start indexing realtime changes.
	// !!! MUST HAPPEN PRIOR TO BACKFILLING !!!
	listener, err := db.Log().Listen()
	if err != nil {
		return nil, nil, err
	}

	defer common.RunIf(func() { listener.Close() })(err)
	dirIndexEvents(
		changeStreamToEventStream(
			self, listener.ch), dir)

	// Grab all the changes from the database
	chgs, err := db.Log().All()
	if err != nil {
		return nil, nil, err
	}

	dir.Add(self)
	dir.Apply(changesToEvents(self, chgs))
	return dir, listener, nil
}

// Returns a newly initialized disseminator.
func replicaInitDissem(ctx common.Context, logger common.Logger, self member, dir *directory) (*disseminator, error) {
	dissem, err := newDisseminator(ctx, logger, self, dir)
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

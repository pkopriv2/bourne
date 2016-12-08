package convoy

import (
	"strconv"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// The replica instance.  This is what ultimately implements the
// "Cluster" abstraction for actual members of the cluster group.
type replica struct {
	// the central context.
	Ctx common.Context

	// the root logger.  decorated with self's information.
	Logger common.Logger

	// the db hosted by this instance.
	Db Database

	// the member hosted by this instance.
	Self *member

	// the central directory - contains the local view of all replica's dbs
	Dir *directory

	// the disseminator.  Responsible for pushing and pulling data
	Dissem *disseminator

	// the view log.  maintains a listing of how many times an event has been viewed.
	ViewLog *viewLog

	// the time log. maintains a time based buffer of events - used for reconciliation
	TimeLog *timeLog

	// the core network server.
	Server net.Server

	Closer chan struct{}
	Closed chan struct{}
}

// Initializes and returns a generic replica instance.
func newReplica(ctx common.Context, db Database, port int) (*replica, error) {

	// The replica will be inextricably bound to itself.
	self, err := replicaInitSelf(ctx, db, port)
	if err != nil {
		return nil, errors.Wrap(err, "Error initializing self")
	}

	// Decorate the root logger with the 'self' instance
	logger := replicaInitLogger(ctx, self)
	logger.Info("Starting replica.")

	dir, err := replicaInitDir(ctx, logger, db, self)
	if err != nil {
		return nil, errors.Wrap(err, "Error initializing directory")
	}

	diss, err := replicaInitDissem(ctx, logger, self, dir)
	if err != nil {
		return nil, errors.Wrap(err, "Error initializing disseminator")
	}

	server, err := replicaInitServer(ctx, logger, self, dir, diss, port)
	if err != nil {
		return nil, errors.Wrap(err, "Error initializing server")
	}

	return &replica{
		Self:   self,
		Ctx:    ctx,
		Dir:    dir,
		Db:     db,
		Logger: logger,
		Dissem: diss,
		Server: server,
		Closer: make(chan struct{}, 1),
		Closed: make(chan struct{})}, nil
}

func (r *replica) Close() error {
	select {
	case <-r.Closed:
		return errors.New("Replica Closed")
	case r.Closer <- struct{}{}:
	}

	r.Logger.Info("Replica shutting down.")
	r.Server.Close()

	// r.Dir.Apply(&memberDelEvent{r.Self.Id, r.Self.Version}, true)
	r.Dissem.Close()
	r.Dir.Close()
	close(r.Closed)
	return nil
}

func (r *replica) Id() uuid.UUID {
	return r.Self.Id
}

func (r *replica) Client() (*client, error) {
	return replicaClient(r.Server)
}

// Helper functions

// Returns the member representing "self"
func replicaInitSelf(ctx common.Context, db Database, port int) (mem *member, err error) {
	var id uuid.UUID
	var seq int
	id, err = db.Log().Id()
	if err != nil {
		return
	}

	seq, err = db.Log().Seq()
	if err != nil {
		return
	}

	mem = newMember(id, "localhost", strconv.Itoa(port), seq, Alive)
	return
}

// Returns a logger decorated with membership info.
func replicaInitLogger(ctx common.Context, self *member) common.Logger {
	return ctx.Logger().Fmt(self.String())
}

// Returns a newly initialized directory that is populated with the given db and member
// and is indexing realtime changes to the db.
func replicaInitDir(ctx common.Context, logger common.Logger, db Database, self *member) (*directory, error) {
	dir := newDirectory(ctx, logger)

	// start indexing realtime changes.
	// !!! MUST HAPPEN PRIOR TO READING LOG CHANGES !!!
	dirIndexEvents(
		changeStreamToEventStream(
			self, changeLogListen(db.Log())), dir)

	// Grab all the changes from the database
	chgs, err := db.Log().All()
	if err != nil {
		return nil, err
	}

	// // Add the self instance to the directory
	// dir.update(func(u *dirUpdate) {
	// u.AddMember(self)
	// })

	// Apply all the changes
	dir.ApplyAll(changesToEvents(self, chgs))

	// Done.
	return dir, nil
}

// Returns a newly initialized disseminator.
func replicaInitDissem(ctx common.Context, logger common.Logger, self *member, dir *directory) (*disseminator, error) {
	// dissem, err := newDisseminator(ctx, logger, self, dir, time.Second)
	// if err != nil {
	// return nil, errors.Wrap(err, "Error constructing disseminator")
	// }
	//
	// // Start disseminating realtime changes.
	// dissemEvents(timeLogListen(dir.Log), dissem)
	// return dissem, nil
	return nil, nil
}

// Returns a newly initialized server.
func replicaInitServer(ctx common.Context, log common.Logger, self *member, dir *directory, dissem *disseminator, port int) (net.Server, error) {
	return newServer(ctx, log, self, dir, dissem, port)
}

// Reconciles a directory
func replicaReconcile(dir *directory, peer *client) error {

	// Send self's directory to the peer.
	_, err := peer.DirApply(dir.Events())
	if err != nil {
		return errors.Wrap(err, "Error publishing events to peer")
	}

	// Download the peer's directory.
	events, err := peer.DirList()
	if err != nil {
		return errors.Wrap(err, "Error retrieving directory list from peer")
	}

	dir.ApplyAll(events)
	return nil
}

// Joins the replica to the given peer.
func replicaJoin(self *replica, peer *client) error {
	return nil

	// Register self with the peer.  (Should result in realtime updates being delivered to self.)
	// _, err := peer.DirApply([]event{newMemberAddEvent(self.Self)})
	// if err != nil {
	// return errors.Wrap(err, "Error registering self with peer")
	// }
	//
	// return replicaReconcile(self.Dir, peer)
}

func replicaClient(server net.Server) (*client, error) {
	raw, err := server.Client()
	if err != nil {
		return nil, err
	}

	return &client{raw}, nil
}

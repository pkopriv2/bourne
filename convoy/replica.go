package convoy

import (
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

const (
	ReplicaPortKey     string = "bourne.convoy.replica.port"
	ReplicaPortDefault string = "8080"
)

// The replica instance.  This is what ultimately implements the
// "Cluster" abstraction for actual members of the cluster group.
type replica struct {
	Ctx    common.Context
	Logger common.Logger
	Db     Database
	Dir    *directory
	Self   *member
	Dissem *disseminator
	Server net.Server

	Closer chan struct{}
	Closed chan struct{}
	Wait   sync.WaitGroup
}

func replicaJoin(self *replica, peer *client) error {

	// Register self with the peer.  (Should result in realtime updates being delivered to self.)
	_, err := peer.DirApply([]event{addMemberEvent(self.Self)})
	if err != nil {
		return errors.Wrap(err, "Error registering self with peer")
	}

	// Send self's directory to the peer.
	_, err = peer.DirApply(self.Dir.Events())
	if err != nil {
		return errors.Wrap(err, "Error publishing events to peer")
	}

	// Download the peer's directory.
	events, err := peer.DirList()
	if err != nil {
		return errors.Wrap(err, "Error retrieving directory list from peer")
	}

	// Index the peer's events.
	self.Dir.ApplyAll(events)

	return nil
}

func replicaStart(ctx common.Context, db Database, port int) (*replica, error) {
	// Create the 'self' instance.
	self, err := replicaInitSelf(ctx, db, port)
	if err != nil {
		return nil, errors.Wrap(err, "Error initializing self")
	}

	// Initialize the root logger
	root := replicaInitLogger(ctx, self)

	// Create the directory instance.
	dir, err := replicaInitDir(ctx, db, self)
	if err != nil {
		return nil, errors.Wrap(err, "Error initializing directory")
	}

	// Create the disseminator
	diss, err := replicaInitDissem(ctx, root, db, self, dir)
	if err != nil {
		return nil, errors.Wrap(err, "Error initializing disseminator")
	}

	// Create the network server.
	server, err := replicaInitServer(ctx, root, self, dir, diss, port)
	if err != nil {
		return nil, errors.Wrap(err, "Error initializing server")
	}

	// Finally, return the replica
	return &replica{
		Self:   self,
		Ctx:    ctx,
		Dir:    dir,
		Db:     db,
		Logger: root,
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
	r.Dissem.Close()
	r.Server.Close()
	r.Dir.Close()
	close(r.Closed)
	return nil
}

func (r *replica) Seed() (*client, error) {
	return nil, nil
}

func (r *replica) Id() uuid.UUID {
	return r.Self.Id
}

func (r *replica) Client() (*client, error) {
	raw, err := r.Server.Client()
	if err != nil {
		return nil, err
	}

	return &client{raw}, nil
}

func (r *replica) Scan(fn func(*amoeba.Scan, uuid.UUID, string, string)) {
	r.Dir.View(func(v *dirView) {
		replicaScanDir(v, fn)
	})
}

func (r *replica) Collect(filter func(id uuid.UUID, key string, val string) bool) (members []Member) {
	r.Dir.View(func(v *dirView) {
		members = replicaFilterDir(v, filter)
	})
	return
}

func (r *replica) GetMember(id uuid.UUID) (mem Member, err error) {
	r.Dir.View(func(v *dirView) {
		mem = v.GetMember(id)
	})
	return
}

// Helper functions
type replicaEnv struct {
	Ctx    common.Context
	Logger common.Logger
	Self   *member
	Dir    *directory
	Dissem *disseminator
}

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

	mem = newMember(id, "localhost", strconv.Itoa(port), seq)
	return
}

// Returns a logger decorated with membership info.
func replicaInitLogger(ctx common.Context, self *member) common.Logger {
	return ctx.Logger().Fmt(self.String())
}

// Returns a newly initialized directory that is populated with the given db and member
// and is indexing realtime changes to the db.
func replicaInitDir(ctx common.Context, db Database, self *member) (*directory, error) {
	dir := newDirectory(ctx)

	// Grab the id
	id, err := db.Log().Id()
	if err != nil {
		return nil, err
	}

	// start indexing realtime changes.
	// !!! MUST HAPPEN PRIOR TO READING LOG CHANGES !!!
	dirIndexEvents(
		changeStreamToEventStream(
			id, changeLogListen(db.Log())), dir)

	// Grab all the changes from the database
	chgs, err := db.Log().All()
	if err != nil {
		return nil, err
	}

	// Add the self instance to the directory
	dir.update(func(u *dirUpdate) {
		u.AddMember(self)
	})

	// Apply all the changes
	dir.ApplyAll(changesToEvents(self.Id, chgs))

	// Done.
	return dir, nil
}

func replicaInitDissem(ctx common.Context, logger common.Logger, db Database, self *member, dir *directory) (*disseminator, error) {
	// Create the disseminator
	dissem, err := newDisseminator(ctx, logger, self, dir, time.Second)
	if err != nil {
		return nil, errors.Wrap(err, "Error constructing disseminator")
	}

	// Start disseminating realtime changes.
	dissemEvents(
		changeStreamToEventStream(
			self.Id, changeLogListen(db.Log())), dissem)

	return dissem, nil
}

// Returns a newly initialized server.
func replicaInitServer(ctx common.Context, log common.Logger, self *member, dir *directory, diss *disseminator, port int) (net.Server, error) {
	return net.NewTcpServer(ctx, log, strconv.Itoa(port), replicaInitHandler(ctx, log, self, dir, diss))
}

// Returns a new service handler for the replica
func replicaInitHandler(ctx common.Context, logger common.Logger, self *member, dir *directory, diss *disseminator) net.Handler {
	env := &replicaEnv{
		Ctx:    ctx,
		Logger: logger.Fmt("ReplicaServer"),
		Self:   self,
		Dir:    dir,
		Dissem: diss}

	return func(req net.Request) net.Response {
		action, err := readMeta(req.Meta())
		if err != nil {
			return net.NewErrorResponse(errors.Wrap(err, "Error parsing action"))
		}

		switch action {
		default:
			return net.NewErrorResponse(errors.Errorf("Unknown action %v", action))
		case epDirApply:
			return replicaDirApply(env, req)
		case epDirList:
			return replicaDirList(env, req)
		}
	}
}

// Handles a dir list request
func replicaDirList(env *replicaEnv, req net.Request) net.Response {
	return newDirListResponse(env.Dir.Events())
}

// Handles a dir apply request
func replicaDirApply(env *replicaEnv, req net.Request) net.Response {
	events, err := readDirApplyRequest(req)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	if len(events) == 0 {
		return net.NewErrorResponse(errors.New("No events to apply!"))
	}

	env.Logger.Debug("Applying [%v] events", len(events))
	successes := env.Dir.ApplyAll(events)

	forward := make([]event, 0, len(successes))
	for i, b := range successes {
		if b {
			forward = append(forward, events[i])
		}
	}

	env.Logger.Debug("Successfully applied [%v] events", len(forward))
	if err := env.Dissem.Push(forward); err != nil {
		return net.NewErrorResponse(err)
	}

	return newDirApplyResponse(successes)
}

func replicaScanDir(v *dirView, fn func(*amoeba.Scan, uuid.UUID, string, string)) {
	v.Scan(func(s *amoeba.Scan, id uuid.UUID, key string, val string, _ int) {
		fn(s, id, key, val)
	})
}

func replicaFilterDir(v *dirView, filter func(uuid.UUID, string, string) bool) (members []Member) {
	ids := make(map[uuid.UUID]struct{})

	v.Scan(func(s *amoeba.Scan, id uuid.UUID, key string, val string, ver int) {
		if filter(id, key, val) {
			ids[id] = struct{}{}
		}
	})

	members = make([]Member, 0, len(ids))

	for id, _ := range ids {
		m := v.GetMember(id)
		if m != nil {
			members = append(members, m)
		}
	}

	return
}

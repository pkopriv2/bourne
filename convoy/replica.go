package convoy

import (
	"strconv"
	"sync"

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
	Dir    *directory
	Db     Database
	Server net.Server
	Self   *member

	Closer chan struct{}
	Closed chan struct{}
	Wait   sync.WaitGroup
}

func addMemberEvent(m *member) event {
	return &memberEvent{m.Id, m.Host, m.Port, m.Version, false}
}

func JoinReplica(self *replica, peer *client) error {

	// 1. Register self with the peer.  (Should result in realtime updates being delivered to self.)
	_, err := peer.DirApply([]event{addMemberEvent(self.Self)})
	if err != nil {
		return errors.Wrap(err, "Error registering self with peer")
	}

	// 2. Download the peer's directory.
	events, err := peer.DirList()
	if err != nil {
		return errors.Wrap(err, "Error retrieving directory list from peer")
	}

	// 3. Index the peer's events.
	self.Dir.ApplyAll(events)

	// 4. Send self's directory to the peer.
	_, err = peer.DirApply(self.Dir.Events())
	if err != nil {
		return errors.Wrap(err, "Error publishing events to peer")
	}

	return nil
}

func StartReplica(ctx common.Context, db Database, port int) (*replica, error) {

	// Create the directory instance.
	dir, err := replicaInitDir(ctx, db.Log())
	if err != nil {
		return nil, err
	}

	// Grab all the change from the log
	chgs, err := db.Log().All()
	if err != nil {
		return nil, err
	}

	// Create the 'self' instance.
	self, err := replicaInitSelf(ctx, db, strconv.Itoa(port))
	if err != nil {
		return nil, err
	}

	// Initialize/populate the directory
	dir.Update(func(u *dirUpdate) {
		u.AddMember(self)
	})

	dir.ApplyAll(changesToEvents(self.Id, chgs))

	// Start the server.
	server, err := net.NewTcpServer(ctx, strconv.Itoa(port), newReplicaHandler(dir))
	if err != nil {
		return nil, err
	}

	return &replica{
		Self:   self,
		Ctx:    ctx,
		Dir:    dir,
		Db:     db,
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

	r.Ctx.Logger().Info("Replica shutting down.")

	r.Server.Close()
	r.Dir.Close()
	close(r.Closed)
	return nil
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
		v.Scan(func(s *amoeba.Scan, id uuid.UUID, key string, val string, _ int) {
			fn(s, id, key, val)
		})
	})
}

func (r *replica) Collect(filter func(id uuid.UUID, key string, val string) bool) []Member {
	var members []Member

	r.Dir.View(func(v *dirView) {
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
	})

	return members
}

func (r *replica) GetMember(id uuid.UUID) (mem Member, err error) {
	r.Dir.View(func(v *dirView) {
		mem = v.GetMember(id)
	})
	return
}

// Helper functions

// Primary service handler for a replica.
func newReplicaHandler(dir *directory) net.Handler {
	logger := dir.Ctx.Logger()

	return func(req net.Request) net.Response {
		action, err := readMeta(req.Meta())
		if err != nil {
			return net.NewErrorResponse(errors.Wrap(err, "Error parsing action"))
		}

		logger.Debug("Processing request: %v", req)
		switch action {
		default:
			return net.NewErrorResponse(errors.Errorf("Unknown action %v", action))
		case epDirApply:
			return replicaHandleDirApply(dir, req)
		case epDirList:
			return replicaHandleDirList(dir, req)
		}
	}
}

// Handles a dir list request
func replicaHandleDirList(dir *directory, req net.Request) net.Response {
	return newDirListResponse(dir.Events())
}

// Handles a dir apply request
func replicaHandleDirApply(replicaHandleDirApply *directory, req net.Request) net.Response {
	events, err := readDirApplyRequest(req)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return newDirApplyResponse(replicaHandleDirApply.ApplyAll(events))
}

// Returns the "self" member.
func replicaInitSelf(ctx common.Context, db Database, port string) (mem *member, err error) {
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

	mem = newMember(id, "localhost", port, seq)
	return
}

// Initializes an empty directory that is indexing all changes from the input
func replicaInitDir(ctx common.Context, log ChangeLog) (*directory, error) {
	dir := newDirectory(ctx)

	// Grab the id
	id, err := log.Id()
	if err != nil {
		return nil, err
	}

	// start indexing realtime changes.
	// !!! MUST HAPPEN PRIOR TO READING LOG CHANGES !!!
	indexEvents(
		changeStreamToEventStream(
			id, changeLogListen(log)), dir)

	return dir, nil
}

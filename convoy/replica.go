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

func StartCluster(ctx common.Context, db Database, port int) (*replica, error) {

	// Create the member instance.
	self, err := replicaSelf(ctx, db, strconv.Itoa(port))
	if err != nil {
		return nil, err
	}

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

	// Add the member entry to the directory
	dir.Update(func(u *update) {
		u.AddMember(self)
	})

	// Index the log
	dir.ApplyAll(changesToEvents(self.Id, chgs))

	// Host the replica
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

	r.Server.Close()
	r.Dir.Close()
	close(r.Closed)
	return nil
}

func (r *replica) Id() uuid.UUID {
	return r.Self.Id
}

func (r *replica) Client() (*client, error) {
	return r.Self.Client(r.Ctx)
}

func (r *replica) Search(filter func(id uuid.UUID, key string, val string) bool) []Member {
	var members []Member

	r.Dir.View(func(v *view) {
		ids := make(map[uuid.UUID]struct{})

		v.Scan(func(s *amoeba.Scan, id uuid.UUID, key string, val string, _ int) {
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
	r.Dir.View(func(v *view) {
		mem = v.GetMember(id)
	})
	return
}

// Helper functions

// Primary service handler for a replica.
func newReplicaHandler(dir *directory) net.Handler {
	return func(req net.Request) net.Response {
		action, err := readMeta(req.Meta())
		if err != nil {
			return net.NewErrorResponse(errors.Wrap(err, "Error parsing action"))
		}

		switch action {
		default:
			return net.NewErrorResponse(errors.Errorf("Unknown action %v", action))
		case epDirApply:
			return replicaHandleDirList(dir, req)
		case epDirList:
			return replicaHandleDirApply(dir, req)
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
func replicaSelf(ctx common.Context, db Database, port string) (mem *member, err error) {
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

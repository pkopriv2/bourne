package convoy

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

var PeerClosedError = errors.New("ERROR:PEER:CLOSED")

type replica struct {
	ctx common.Context
	dir *directory
	log *changeLog

	closer chan struct{}
	closed chan struct{}
	wait   sync.WaitGroup
}

func StartReplica(ctx common.Context) (*replica, error) {
	stash, err := stash.OpenConfigured(ctx)
	if err != nil {
		return nil, err
	}

	r := &replica{
		ctx: ctx,
		dir: newDirectory(ctx),
		log: openChangeLog(stash),
		closer: make(chan struct{}, 1),
		closed: make(chan struct{})}

	return r, r.start()
}

func (r *replica) start() error {
	// Grab the change log id.
	id, err := r.log.Id()
	if err != nil {
		return err
	}

	// Grab all the changes from the log
	chgs, err := r.log.All()
	if err != nil {
		return err
	}

	// Start realtime processing of changes.
	indexEvents(
		changeStreamToEventStream(
			id, changeLogListen(r.log)), r.dir)

	// Finally, build the directory from the changelog
	r.dir.ApplyAll(changesToEvents(id, chgs))

	// Now that the internal index has been built, host it.
	server, err := net.NewTcpServer(r.ctx, 0, newReplicaHandler(r.dir))
	if err != nil {
		return nil
	}

	r.wait.Add(1)
	go func() {
		defer r.wait.Done()
		<-r.closed
		server.Close()
	}()
	return nil
}

func (r *replica) GetMember(id uuid.UUID) (Member, error) {
	var member Member
	r.dir.View(func(v *view) {
		v.GetMember(id)
	})
	return member, nil
}

func (p *replica) Close() error {
	select {
	case <-p.closed:
		return PeerClosedError
	case p.closer <- struct{}{}:
	}

	close(p.closed)
	p.wait.Wait()
	return nil
}

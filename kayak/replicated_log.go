package kayak

import (
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

// The primary replicated log machine abstraction.
//
// Internally, this consists of a single member object that hosts all the member
// state.  The internal instance is free to move between the various engine
// components. Each sub machine is responsible for understanding the conditions that
// lead to inter-machine movement.  Requests should be forwarded to the member
// instance, except where the instance has exposed public methods.
//
type replicatedLog struct {
	// the internal member instance.  This is guaranteed to exist in at MOST one sub-machine
	replica *replica

	// the follower sub-machine
	follower *followerSpawner

	// the candidate sub-machine
	candidate *candidateSpawnwer

	// the leader sub-machine
	leader *leaderSpawner
}

func newReplicatedLog(ctx common.Context, logger common.Logger, self string, store LogStore, db *bolt.DB) (*replicatedLog, error) {
	rep, err := newReplica(ctx, logger, self, store, db)
	if err != nil {
		return nil, err
	}

	follower := make(chan *replica)
	candidate := make(chan *replica)
	leader := make(chan *replica)

	m := &replicatedLog{
		replica:   rep,
		follower:  newFollowerSpawner(follower, candidate, rep.closed),
		candidate: newCandidateSpawner(ctx, candidate, leader, follower, rep.closed),
		leader:    newLeaderSpawner(ctx, leader, follower, rep.closed),
	}
	m.start()
	return m, nil
}

func (h *replicatedLog) Close() error {
	return h.replica.Close()
}

func (h *replicatedLog) start() error {
	h.follower.in <- h.replica
	return nil
}

func (h *replicatedLog) Context() common.Context {
	return h.replica.Ctx
}

func (h *replicatedLog) Self() peer {
	return h.replica.Self
}

func (h *replicatedLog) Peers() []peer {
	return h.replica.Peers()
}

func (h *replicatedLog) Log() *eventLog {
	return h.replica.Log
}

func (h *replicatedLog) CurrentTerm() term {
	return h.replica.CurrentTerm()
}

func (h *replicatedLog) Replicate(id uuid.UUID, term int, prevLogIndex int, prevLogTerm int, batch []LogItem, commit int) (response, error) {
	return h.replica.Replicate(id, term, prevLogIndex, prevLogTerm, batch, commit)
}

func (h *replicatedLog) RequestVote(id uuid.UUID, term int, logIndex int, logTerm int) (response, error) {
	return h.replica.RequestVote(id, term, logIndex, logTerm)
}

func (h *replicatedLog) RemoteAppend(event Event, source uuid.UUID, seq int, kind int) (LogItem, error) {
	return h.replica.RemoteAppend(event, source, seq, kind)
}

func (h *replicatedLog) LocalAppend(event Event, source uuid.UUID, seq int, kind int) (LogItem, error) {
	return h.replica.LocalAppend(event, source, seq, kind)
}

func (r *replicatedLog) Append(e Event, kind int) (LogItem, error) {
	return r.replica.Append(e, kind)
}

func (r *replicatedLog) Listen(start int, buf int) (Listener, error) {
	return r.replica.Listen(start, buf)
}

func (r *replicatedLog) Applied(int) {
	panic("not implemented")
}

func (r *replicatedLog) SyncTimeout(timeout time.Duration) error {
	panic("not implemented")
}

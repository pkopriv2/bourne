package kayak

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

// The primary host machine abstraction.
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

func newReplicatedLog(ctx common.Context, logger common.Logger, self peer, others []peer, parser Parser, stash stash.Stash) (*replicatedLog, error) {
	rep, err := newReplica(ctx, logger, self, others, parser, openTermStorage(stash))
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
	return h.replica.peers
}

func (h *replicatedLog) Parser() Parser {
	return h.replica.Parser
}

func (h *replicatedLog) Log() *eventLog {
	return h.replica.Log
}

func (h *replicatedLog) CurrentTerm() term {
	return h.replica.CurrentTerm()
}

func (h *replicatedLog) InnerAppend(id uuid.UUID, term int, prevLogIndex int, prevLogTerm int, batch []Event, commit int) (response, error) {
	return h.replica.Replicate(id, term, prevLogIndex, prevLogTerm, batch, commit)
}

func (h *replicatedLog) RequestVote(id uuid.UUID, term int, logIndex int, logTerm int) (response, error) {
	return h.replica.RequestVote(id, term, logIndex, logTerm)
}

func (h *replicatedLog) RemoteAppend(event Event) (LogItem, error) {
	return h.replica.RemoteAppend(event)
}

func (h *replicatedLog) LocalAppend(event Event) (LogItem, error) {
	return h.replica.LocalAppend(event)
}

func (r *replicatedLog) Append(e Event) (LogItem, error) {
	return r.LocalAppend(e)
}

func (r *replicatedLog) Listen(from int, buf int) (Listener, error) {
	return r.Log().ListenCommits(from, buf)
}

func (r *replicatedLog) ListenLive(buf int) (Listener, error) {
	return r.Log().ListenCommitsLive(buf)
}

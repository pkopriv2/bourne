package kayak

import (
	"fmt"

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

	// closing utilities.
	closed chan struct{}

	// closing lock.  (a buffered channel of 1 entry.)
	closer chan struct{}
}

func newReplicatedLog(ctx common.Context, logger common.Logger, self peer, others []peer, parser Parser, stash stash.Stash) (*replicatedLog, error) {
	rep, err := newReplica(ctx, logger, self, others, parser, openTermStorage(stash))
	if err != nil {
		return nil, err
	}

	follower := make(chan *replica)
	candidate := make(chan *replica)
	leader := make(chan *replica)
	closed := make(chan struct{})
	closer := make(chan struct{}, 1)

	m := &replicatedLog{
		replica:   rep,
		follower:  newFollowerSpawner(follower, candidate, closed),
		candidate: newCandidateSpawner(ctx, candidate, leader, follower, closed),
		leader:    newLeaderSpawner(ctx, leader, follower, closed),
		closed:    closed,
		closer:    closer,
	}
	m.start()
	return m, nil
}

func (h *replicatedLog) Close() error {
	select {
	case <-h.closed:
		return ClosedError
	case h.closer <- struct{}{}:
	}

	h.replica.Close()
	close(h.closed)
	return nil
}

func (h *replicatedLog) start() {
	h.follower.in <- h.replica
}

func (h *replicatedLog) Context() common.Context {
	return h.replica.Ctx
}

func (h *replicatedLog) Commits() <-chan Event {
	return nil
	// return h.replica.Log.Commits()
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

func (h *replicatedLog) RequestAppendEvents(id uuid.UUID, term int, prevLogIndex int, prevLogTerm int, batch []Event, commit int) (response, error) {
	return h.replica.RequestAppendEvents(id, term, prevLogIndex, prevLogTerm, batch, commit)
}

func (h *replicatedLog) RequestVote(id uuid.UUID, term int, logIndex int, logTerm int) (response, error) {
	return h.replica.RequestVote(id, term, logIndex, logTerm)
}

func (h *replicatedLog) MachineProxyAppend(event Event) (int, error) {
	return h.replica.MachineProxyAppend(event)
}

func (h *replicatedLog) MachineAppend(event Event) (int, error) {
	return h.replica.MachineAppend(event)
}

// Internal request vote.  Requests are put onto the internal member
// channel and consumed by the currently active sub-machine.
//
// Request votes ONLY come from members who are candidates.
type requestVote struct {
	id          uuid.UUID
	term        int
	maxLogTerm  int
	maxLogIndex int
	ack         chan response
}

func (r requestVote) String() string {
	return fmt.Sprintf("RequestVote(%v,%v)", r.id.String()[:8], r.term)
}

func (r requestVote) reply(term int, success bool) {
	r.ack <- response{term, success}
}


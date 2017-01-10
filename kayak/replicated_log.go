package kayak

import (
	"fmt"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

type listener struct {
	raw *eventLogListener
	items chan LogItem
}

func newListener(raw *eventLogListener) *listener {
	l := &listener{raw, make(chan LogItem, 8)}
	go func() {
		for {
			select {
			case <-l.raw.closed:
				return
			case i := <-l.raw.Items():
				select {
				case <-l.raw.closed:
					return
				case l.items<-LogItem{i.index, i.event}:
				}
			}
		}
	}()
	return l
}

func (l *listener) Close() error {
	return l.raw.Close()
}

func (l *listener) Items() <-chan LogItem {
	return l.items
}

func (l *listener) Closed() <-chan struct{} {
	return l.raw.closed
}

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
	listener, err := h.Log().Listen()
	if err != nil {
		return err
	}

	go func() {
		defer listener.Close()
		for {
			select {
			case <-h.replica.closed:
				return
			case <-listener.Closed():
				return
			case i := <-listener.Items():
				h.replica.Logger.Info("Commit!: %v", i)
				// h.replica.Machine.Commit(LogItem{i.index, i.event})
			}
		}
	}()

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

// Public apis
func (r *replicatedLog) Listen() (Listener, error) {
	l, err := r.Log().Listen()
	if err != nil {
		return nil, err
	}

	return newListener(l), nil
}

func (r *replicatedLog) Append(e Event) (LogItem, error) {
	index, err := r.MachineAppend(e)
	if err != nil {
		return LogItem{}, err
	}

	return LogItem{index, e}, nil
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

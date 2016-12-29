package kayak

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

// The primary host machine abstraction.
//
// Internally, this consists of a single member object that hosts all the member
// state.  The internal instance is free to move between the various engine
// components. Each sub machine is responsible for understanding the conditions that
// lead to inter-machine movement.  Each machine defines its own concurrency semantics,
// therefore it is NOT generally safe to access the internal member state.
//
type engine struct {

	// the internal member instance.  This is guaranteed to exist in at MOST
	instance *member

	// the follower sub-machine
	follower *follower

	// the candidate sub-machine
	candidate *candidate

	// the leader sub-machine
	leader *leader

	// closing utilities.
	closed chan struct{}

	// closing lock.  (a buffered channel of 1 entry.)
	closer chan struct{}
}

func newHostEngine(ctx common.Context, logger common.Logger, self peer, others []peer, parser Parser) (*engine, error) {
	follower := make(chan *member)
	candidate := make(chan *member)
	leader := make(chan *member)

	closed := make(chan struct{})
	closer := make(chan struct{}, 1)

	inst := &member{
		ctx:           ctx,
		id:            self.id,
		self:          self,
		peers:         others,
		logger:        logger,
		parser:        parser,
		log:           newEventLog(ctx),
		appends:       make(chan appendEvents),
		votes:         make(chan requestVote),
		clientAppends: make(chan clientAppend),
		timeout:       time.Millisecond * time.Duration((rand.Intn(1000) + 1000)),
	}

	m := &engine{
		instance:  inst,
		follower:  newFollower(ctx, follower, candidate, closed),
		candidate: newCandidate(ctx, candidate, leader, follower, closed),
		leader:    newLeader(ctx, leader, follower, closed),
		closed:    closed,
		closer:    closer,
	}

	if err := m.start(); err != nil {
		return nil, err
	}

	return m, nil
}

func (h *engine) Close() error {
	select {
	case <-h.closed:
		return ClosedError
	case h.closer <- struct{}{}:
	}

	close(h.closed)
	return nil
}

func (h *engine) start() error {
	return h.follower.transition(h.instance, h.follower.in)
}

func (h *engine) Context() common.Context {
	return h.instance.ctx
}

func (h *engine) Self() peer {
	return h.instance.self
}

func (h *engine) Peers() []peer {
	return h.instance.peers
}

func (h *engine) Parser() Parser {
	return h.instance.parser
}

func (h *engine) Log() *eventLog {
	return h.instance.log
}

func (h *engine) CurrentTerm() term {
	return h.instance.CurrentTerm()
}

func (h *engine) RequestAppendEvents(id uuid.UUID, term int, prevLogIndex int, prevLogTerm int, batch []event, commit int) (response, error) {
	append := appendEvents{
		id, term, prevLogIndex, prevLogTerm, batch, commit, make(chan response, 1)}

	select {
	case <-h.closed:
		return response{}, ClosedError
	case h.instance.appends <- append:
		select {
		case <-h.closed:
			return response{}, ClosedError
		case r := <-append.ack:
			return r, nil
		}
	}
}

func (h *engine) RequestVote(id uuid.UUID, term int, logIndex int, logTerm int) (response, error) {
	req := requestVote{id, term, logIndex, logTerm, make(chan response, 1)}

	select {
	case <-h.closed:
		return response{}, ClosedError
	case h.instance.votes <- req:
		select {
		case <-h.closed:
			return response{}, ClosedError
		case r := <-req.ack:
			return r, nil
		}
	}
}

func (h *engine) RequestClientAppend(events []event) error {
	append := clientAppend{events, make(chan error, 1)}

	select {
	case <-h.closed:
		return ClosedError
	case h.instance.clientAppends <- append:
		select {
		case <-h.closed:
			return ClosedError
		case r := <-append.ack:
			return r
		}
	}
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

func (r requestVote) reply(term int, success bool) bool {
	select {
	case r.ack <- response{term, success}:
		return true
	default:
		return false // shouldn't be possible
	}
}

// Internal append events request.  Requests are put onto the internal member
// channel and consumed by the currently active sub-machine.
//
// Append events ONLY come from members who are leaders. (Or think they are leaders)
type appendEvents struct {
	id           uuid.UUID
	term         int
	prevLogIndex int
	prevLogTerm  int
	events       []event
	commit       int
	ack          chan response
}

func (a appendEvents) String() string {
	return fmt.Sprintf("AppendEvents(id=%v,prevIndex=%v,prevTerm%v,items=%v)", a.id.String()[:8], a.prevLogIndex, a.prevLogTerm, len(a.events))
}

func (a *appendEvents) reply(term int, success bool) bool {
	select {
	case a.ack <- response{term, success}:
		return true
	default:
		return false // shouldn't be possible
	}
}

// Internal client append request.  Requests are put onto the internal member
// channel and consumed by the currently active sub-machine.
//
// These come from active clients.
type clientAppend struct {
	events []event
	ack    chan error
}

func (a clientAppend) String() string {
	return fmt.Sprintf("ClientAppend(%v)", len(a.events))
}

func (a *clientAppend) reply(err error) bool {
	select {
	case a.ack <- err:
		return true
	default:
		return false // shouldn't be possible
	}
}

// Internal response type.  These are returned through the
// request 'ack'/'response' channels by the currently active
// sub-machine component.
type response struct {
	term    int
	success bool
}

package kayak

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// The primary member machine abstraction.
//
// Internally, this consists of a single instance object that hosts all the member
// state.  The internal instance is free to move between the various sub machines.
// Each machine is responsible for understanding the conditions that lead to
// inter-machine movement.  Each machine defines its own concurrency semantics,
// therefore it is NOT generally safe to access the internal instance state.
//
type member struct {

	// logger instance
	logger common.Logger

	// the internal member instance.  This is guaranteed to exist in at MOST
	instance *instance

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

func newMember(ctx common.Context, logger common.Logger, self peer, others []peer) (*member, error) {
	followerIn := make(chan *instance)
	candidateIn := make(chan *instance)
	leaderIn := make(chan *instance)
	closed := make(chan struct{})
	closer := make(chan struct{}, 1)

	inst := &instance{
		ctx:     ctx,
		id:      self.id,
		self:    self,
		peers:   others,
		log:     newViewLog(ctx),
		appends: make(chan appendEvents),
		votes:   make(chan requestVote),
		timeout: time.Millisecond * time.Duration((rand.Intn(1000) + 1000)),
	}

	m := &member{
		instance:  inst,
		follower:  newFollower(ctx, followerIn, candidateIn, closed),
		candidate: newCandidate(ctx, candidateIn, leaderIn, followerIn, closed),
		leader:    newLeader(ctx, logger, leaderIn, followerIn, closed),
		closed:    closed,
		closer:    closer,
	}

	if err := m.start(); err != nil {
		return nil, err
	}

	return m, nil
}

func (h *member) Close() error {
	select {
	case <-h.closed:
		return ClosedError
	case h.closer <- struct{}{}:
	}
	close(h.closed)
	return nil
}

func (h *member) start() error {
	return h.follower.send(h.instance, h.follower.in)
}

func (h *member) RequestAppendEvents(id uuid.UUID, term int, logIndex int, logTerm int, batch []event, commit int) (response, error) {
	append := appendEvents{
		id, term, logIndex, logTerm, batch, commit, make(chan response, 1)}

	h.logger.Debug("Receiving append events [%v]", append)
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

func (h *member) RequestVote(id uuid.UUID, term int, logIndex int, logTerm int) (response, error) {
	req := requestVote{id, term, logIndex, logTerm, make(chan response, 1)}

	h.logger.Debug("Receiving request vote [%v]", req)
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
	return fmt.Sprintf("AppendEvents(%v,%v)", a.id.String()[:8], a.term)
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

func (r response) Write(w scribe.Writer) {
	w.Write("term", r.term)
	w.Write("success", r.success)
}

func readResponse(r scribe.Reader) (response, error) {
	ret := response{}

	var err error
	err = common.Or(err, r.Read("term", &ret.term))
	err = common.Or(err, r.Read("success", &ret.success))
	return ret, err
}

// A term represents a particular member state in the Raft epochal time model.
type term struct {

	// the current term number (increases monotonically across the cluster)
	num int

	// the current leader (as seen by this member)
	leader *uuid.UUID

	// who was voted for this term (guaranteed not nil when leader != nil)
	votedFor *uuid.UUID
}

func (t term) String() string {
	return fmt.Sprintf("Term(%v,%v,%v)", t.num, t.leader, t.votedFor)
}

// The member is the primary membership identity.  Within the core machine,
// only a single instance ever exists, but its location within the machine
// may change over time.  Therefore all updates/requests must be forwarded
// to the machine currently processing the member.
type instance struct {

	// configuration used to build this instance.
	ctx common.Context

	// the core member logger
	logger common.Logger

	// the unique id of this member.
	id uuid.UUID

	// the peer representing the local instance
	self peer

	// data lock (currently using very coarse lock)
	lock sync.RWMutex

	// the current term.
	term term

	// the other peers. (currently static list)
	peers []peer

	// the election timeout.  (heartbeat: = timeout / 5)
	timeout time.Duration

	// the distributed event log.
	log *eventLog

	// A channel whose elements are the ordered events as they committed.
	committed chan event

	// request vote events.
	votes chan requestVote

	// append requests (from peers)
	appends chan appendEvents

	// append requests (from clients)
	clientAppends chan clientAppend
}

func (h *instance) String() string {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return fmt.Sprintf("%v, %v:", h.self, h.term)
}

func (h *instance) Term(num int, leader *uuid.UUID, vote *uuid.UUID) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.term = term{num, leader, vote}
}

func (h *instance) Peers() []peer {
	h.lock.RLock()
	defer h.lock.RUnlock()
	ret := make([]peer, 0, len(h.peers))
	return append(ret, h.peers...)
}

func (h *instance) Majority() int {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return majority(len(h.peers) + 1)
}

func (h *instance) Broadcast(fn func(c *client) response) <-chan response {
	h.lock.Lock()
	defer h.lock.Unlock()
	peers := h.Peers()

	ret := make(chan response, len(peers))
	for _, p := range peers {
		go func(p peer) {
			cl, err := p.Client(h.ctx)
			if err != nil {
				ret <- response{h.term.num, false}
			}

			fn(cl)
		}(p)
	}
	return ret
}


func majority(num int) int {
	return int(math.Ceil(float64(num) / float64(2)))
}

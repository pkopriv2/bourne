package kayak

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// References:
//
// * https://raft.github.io/raft.pdf
// * https://www.youtube.com/watch?v=LAqyTyNUYSY
// * https://github.com/ongardie/dissertation/blob/master/book.pdf?raw=true
//
//
// Considered a BFA (Byzantine-Federated-Agreement) approach, but looked too complex for our
// initial needs. (consider for future systems)
//
// * https://www.stellar.org/papers/stellar-consensus-protocol.pdf
//
// NOTE: Currently only election is implemented.
// TODO:
//  * Support proper client appends + log impl
//  * Support changing cluster membership
//  * Support durable log!!
//

type event interface {
	scribe.Writable
}

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

// The primary leader RPC type.  These only come from those who *think*
// they are leaders!
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

// the primary consumer request type
type clientAppend struct {
	events []event
	ack    chan error
}

func (a clientAppend) String() string {
	return fmt.Sprintf("AppendLog(%v)", len(a.events))
}

func (a *clientAppend) reply(err error) bool {
	select {
	case a.ack <- err:
		return true
	default:
		return false // shouldn't be possible
	}
}

// standard response type
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

// A peer contains the identifying info of a cluster member.
type peer struct {
	id   uuid.UUID
	addr string
}

func (p peer) String() string {
	return fmt.Sprintf("Peer(%v)", p.id.String()[:8], p.addr)
}

func (p peer) Client(ctx common.Context) (*client, error) {
	raw, err := net.NewTcpClient(ctx, ctx.Logger(), p.addr)
	if err != nil {
		return nil, err
	}

	return &client{raw}, nil
}

// A term represents a particular member state in the RAFT epochal time model.
type term struct {

	// the current term number
	num int

	// the current term information.
	leader *uuid.UUID

	// who was voted for this term
	votedFor *uuid.UUID
}

func (t term) String() string {
	return fmt.Sprintf("Term(%v,%v,%v)", t.num, t.leader, t.votedFor)
}

// The member is the core identity.  Within the core machine, only
// a single instance ever exists, but its location within the machine
// may change between terms.
type member struct {

	// configuration used to build this instance.
	ctx common.Context

	// the unique id of this member.
	id uuid.UUID

	// the peer representing the local instance
	self peer

	// the current term.
	term term

	// the other peers. (currently static list)
	peers []peer

	// data lock (currently using very coarse lock)
	lock sync.RWMutex

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

func (h *member) String() string {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return fmt.Sprintf("%v, %v:", h.self, h.term)
}

func (h *member) Term(num int, leader *uuid.UUID, vote *uuid.UUID) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.term = term{num, leader, vote}
}

func (h *member) Peers() []peer {
	h.lock.Lock()
	defer h.lock.Unlock()
	ret := make([]peer, 0, len(h.peers))
	return append(ret, h.peers...)
}

func (h *member) Majority() int {
	return majority(len(h.peers) + 1)
}

func (h *member) Broadcast(fn func(c *client) response) <-chan response {
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

type core struct {
	ctx common.Context

	logger common.Logger

	// the internal member instance.  This is guaranteed to exist in at MOST
	// one machine at a given time.
	instance *member

	follower *follower

	candidate *candidate

	leader *leader

	// closing utilities.
	closed chan struct{}

	closer chan struct{}
}

func newMember(ctx common.Context, logger common.Logger, self peer, others []peer) (*core, error) {

	// shared channels.
	followerIn := make(chan *member)
	candidateIn := make(chan *member)
	leaderIn := make(chan *member)
	closed := make(chan struct{})
	closer := make(chan struct{}, 1)

	m := &core{
		ctx:    ctx,
		logger: logger,
		instance: &member{
			ctx:     ctx,
			id:      self.id,
			self:    self,
			peers:   others,
			log:     newViewLog(ctx),
			appends: make(chan appendEvents),
			votes:   make(chan requestVote),
			timeout: time.Millisecond * time.Duration((rand.Intn(1000) + 1000)),
		},
		follower:  newFollower(ctx, logger, followerIn, candidateIn, closed),
		candidate: newCandidate(ctx, logger, candidateIn, leaderIn, followerIn, closed),
		leader:    newLeader(ctx, logger, leaderIn, followerIn, closed),
		closed:    closed,
		closer:    closer,
	}

	if err := m.start(); err != nil {
		return nil, err
	}

	return m, nil
}

func (h *core) Close() error {
	select {
	case <-h.closed:
		return ClosedError
	case h.closer <- struct{}{}:
	}
	close(h.closed)
	return nil
}

func (h *core) start() error {
	return h.follower.send(h.instance, h.follower.in)
}

func (h *core) RequestAppendEvents(id uuid.UUID, term int, logIndex int, logTerm int, batch []event, commit int) (response, error) {
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

func (h *core) RequestVote(id uuid.UUID, term int, logIndex int, logTerm int) (response, error) {
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

func majority(num int) int {
	return int(math.Ceil(float64(num) / float64(2)))
}

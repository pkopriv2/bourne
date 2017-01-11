package kayak

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

// The member is the primary identity within a cluster.  Within the core machine,
// only a single instance ever exists, but its location within the machine
// may change over time.  Therefore all updates/requests must be forwarded
// to the machine currently processing the member.
type replica struct {

	// configuration used to build this instance.
	Ctx common.Context

	// the core member logger
	Logger common.Logger

	// // the unique id of this member. (copied for brevity from self)
	Id uuid.UUID

	// the peer representing the local instance
	Self peer

	// the event parser. (used to spawn clients.)
	Parser Parser

	// data lock (currently using very coarse lock)
	lock sync.RWMutex

	// the current term.
	term term

	// the other peers. (currently static list)
	peers []peer

	// the election timeout.  (heartbeat: = timeout / 5)
	ElectionTimeout time.Duration

	// the client timeout
	RequestTimeout time.Duration

	// the replicated state machine.
	Machine Machine

	// the event log.
	Log *eventLog

	// the durable term store.
	terms *storage

	// request vote events.
	VoteRequests chan requestVote

	// append requests (presumably from leader)
	AppendRequests chan appendEvents

	// append requests (from clients)
	RemoteAppends chan localAppend

	// append requests (from local state machine)
	LocalAppends chan localAppend

	// closing utilities
	closed chan struct{}
	closer chan struct{}
}

func newReplica(ctx common.Context, logger common.Logger, self peer, others []peer, parser Parser, terms *storage) (*replica, error) {
	term, err := terms.Get(self.id)
	if err != nil {
		return nil, err
	}

	return &replica{
		Ctx:             ctx,
		Id:              self.id,
		Self:            self,
		peers:           others,
		Logger:          logger,
		Parser:          parser,
		term:            term,
		terms:           terms,
		Log:             newEventLog(ctx),
		AppendRequests:  make(chan appendEvents),
		VoteRequests:    make(chan requestVote),
		RemoteAppends:   make(chan localAppend),
		LocalAppends:    make(chan localAppend),
		ElectionTimeout: time.Millisecond * time.Duration((rand.Intn(2000) + 1000)),
		RequestTimeout:  10 * time.Second,
		closed:          make(chan struct{}),
		closer:          make(chan struct{}, 1),
	}, nil
}

func (h *replica) String() string {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return fmt.Sprintf("%v, %v:", h.Self, h.term)
}

func (h *replica) Term(num int, leader *uuid.UUID, vote *uuid.UUID) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.term = term{num, leader, vote}
	h.Logger.Info("Durably storing updated term [%v]", h.term)
	h.terms.Save(h.Id, h.term)
}

func (h *replica) CurrentTerm() term {
	h.lock.Lock()
	defer h.lock.Unlock()
	return h.term // i assume return is bound prior to the deferred function....
}

func (h *replica) Peer(id uuid.UUID) (peer, bool) {
	for _, p := range h.Cluster() {
		if p.id == id {
			return p, true
		}
	}
	return peer{}, false
}

func (h *replica) Cluster() []peer {
	peers := h.Peers()
	ret := make([]peer, 0, len(peers)+1)
	ret = append(ret, peers...)
	ret = append(ret, h.Self)
	return ret
}

func (h *replica) Peers() []peer {
	h.lock.RLock()
	defer h.lock.RUnlock()
	ret := make([]peer, 0, len(h.peers))
	return append(ret, h.peers...)
}

func (h *replica) Majority() int {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return majority(len(h.peers) + 1)
}

func (h *replica) Broadcast(fn func(c *client) response) <-chan response {
	peers := h.Peers()

	ret := make(chan response, len(peers))
	for _, p := range peers {
		go func(p peer) {
			cl, err := p.Client(h.Ctx, h.Parser)
			if cl == nil || err != nil {
				ret <- response{h.term.num, false}
				return
			}

			defer cl.Close()
			ret <- fn(cl)
		}(p)
	}
	return ret
}

func (r *replica) Close() error {
	select {
	case <-r.closed:
		return ClosedError
	case r.closer <- struct{}{}:
	}

	close(r.closed)
	return nil
}

func (h *replica) RequestAppendEvents(id uuid.UUID, term int, prevLogIndex int, prevLogTerm int, batch []Event, commit int) (response, error) {
	append := appendEvents{
		id, term, prevLogIndex, prevLogTerm, batch, commit, make(chan response, 1)}

	select {
	case <-h.closed:
		return response{}, ClosedError
	case h.AppendRequests <- append:
		select {
		case <-h.closed:
			return response{}, ClosedError
		case r := <-append.ack:
			return r, nil
		}
	}
}

func (h *replica) RequestVote(id uuid.UUID, term int, logIndex int, logTerm int) (response, error) {
	req := requestVote{id, term, logIndex, logTerm, make(chan response, 1)}

	select {
	case <-h.closed:
		return response{}, ClosedError
	case h.VoteRequests <- req:
		select {
		case <-h.closed:
			return response{}, ClosedError
		case r := <-req.ack:
			return r, nil
		}
	}
}

func (h *replica) RemoteAppend(event Event) (LogItem, error) {
	append := newLocalAppend(event)

	timer := time.NewTimer(h.RequestTimeout)
	select {
	case <-h.closed:
		return LogItem{}, ClosedError
	case <-timer.C:
		return LogItem{}, common.NewTimeoutError(h.RequestTimeout, "ClientAppend")
	case h.RemoteAppends <- append:
		select {
		case <-h.closed:
			return LogItem{}, ClosedError
		case r := <-append.ack:
			return r.Item, r.Err
		case <-timer.C:
			return LogItem{}, common.NewTimeoutError(h.RequestTimeout, "ClientAppend")
		}
	}
}

func (h *replica) LocalAppend(event Event) (LogItem, error) {
	append := localAppend{event, make(chan localAppendResponse, 1)}

	timer := time.NewTimer(h.RequestTimeout)
	select {
	case <-h.closed:
		return LogItem{}, ClosedError
	case <-timer.C:
		return LogItem{}, common.NewTimeoutError(h.RequestTimeout, "ClientAppend")
	case h.LocalAppends <- append:
		select {
		case <-h.closed:
			return LogItem{}, ClosedError
		case r := <-append.ack:
			return r.Item, r.Err
		case <-timer.C:
			return LogItem{}, common.NewTimeoutError(h.RequestTimeout, "ClientAppend")
		}
	}
}

func majority(num int) int {
	return int(math.Ceil(float64(num) / float64(2)))
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
	events       []Event
	commit       int
	ack          chan response
}

func (a appendEvents) String() string {
	return fmt.Sprintf("AppendEvents(id=%v,prevIndex=%v,prevTerm%v,items=%v)", a.id.String()[:8], a.prevLogIndex, a.prevLogTerm, len(a.events))
}

func (a *appendEvents) reply(term int, success bool) {
	a.ack <- response{term, success}
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

// Client append request.  Requests are put onto the internal member
// channel and consumed by the currently active sub-machine.
//
// These come from active clients.
type localAppend struct {
	Event Event
	ack   chan localAppendResponse
}

func newLocalAppend(event Event) localAppend {
	return localAppend{event, make(chan localAppendResponse, 1)}
}

func (a localAppend) Return(item LogItem, err error) {
	a.ack <- localAppendResponse{Item: item, Err: err}
}

func (a localAppend) Fail(err error) {
	a.ack <- localAppendResponse{Err: err}
}

// Client append request.  Requests are put onto the internal member
// channel and consumed by the currently active sub-machine.
//
// These come from active clients.
type localAppendResponse struct {
	Item LogItem
	Err  error
}

// Internal response type.  These are returned through the
// request 'ack'/'response' channels by the currently active
// sub-machine component.
type response struct {
	term    int
	success bool
}

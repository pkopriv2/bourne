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
	Machine StateMachine

	// the event log.
	Log *eventLog

	// the durable term store.
	terms *storage

	// request vote events.
	Votes chan requestVote

	// append requests (presumably from leader)
	LogAppends chan appendEvents

	// append requests (from clients)
	ProxyMachineAppends chan machineAppend

	// append requests (from local state machine)
	MachineAppends chan machineAppend

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
		Ctx:                 ctx,
		Id:                  self.id,
		Self:                self,
		peers:               others,
		Logger:              logger,
		Parser:              parser,
		term:                term,
		terms:               terms,
		Log:                 newEventLog(ctx),
		LogAppends:          make(chan appendEvents),
		Votes:               make(chan requestVote),
		ProxyMachineAppends: make(chan machineAppend),
		ElectionTimeout:     time.Millisecond * time.Duration((rand.Intn(1000) + 1000)),
		RequestTimeout:      10 * time.Second,
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1),
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
	case h.LogAppends <- append:
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
	case h.Votes <- req:
		select {
		case <-h.closed:
			return response{}, ClosedError
		case r := <-req.ack:
			return r, nil
		}
	}
}

func (h *replica) MachineProxyAppend(events Event) (bool, error) {
	append := machineAppend{events, make(chan proxyAppendResponse, 1)}

	timer := time.NewTimer(h.RequestTimeout)
	select {
	case <-h.closed:
		return false, ClosedError
	case <-timer.C:
		return false, NewTimeoutError(h.RequestTimeout, "ClientAppend")
	case h.ProxyMachineAppends <- append:
		select {
		case <-h.closed:
			return false, ClosedError
		case r := <-append.ack:
			return r.success, r.err
		case <-timer.C:
			return false, NewTimeoutError(h.RequestTimeout, "ClientAppend")
		}
	}
}

func (h *replica) MachineAppend(event Event) (bool, error) {
	append := machineAppend{event, make(chan proxyAppendResponse, 1)}

	timer := time.NewTimer(h.RequestTimeout)
	select {
	case <-h.closed:
		return false, ClosedError
	case <-timer.C:
		return false, NewTimeoutError(h.RequestTimeout, "ClientAppend")
	case h.ProxyMachineAppends <- append:
		select {
		case <-h.closed:
			return false, ClosedError
		case r := <-append.ack:
			return r.success, r.err
		case <-timer.C:
			return false, NewTimeoutError(h.RequestTimeout, "ClientAppend")
		}
	}
}

func majority(num int) int {
	return int(math.Ceil(float64(num) / float64(2)))
}

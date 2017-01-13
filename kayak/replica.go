package kayak

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

// The replica is the state container for a member of a cluster.  The
// replica is managed by a single member of the replicated log state machine
// network.  However, the replica is also a machine itself.  Consumers can
// interact with it and it can respond to its own state changes.
//
// The replica is the primary gatekeeper to the external state machine
// and it manages the flow of data to/from it.
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

	// the number of commits before a compaction is triggered.
	SnapshotThreshold int

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
	Replications chan replicateEvents

	// append requests (from clients)
	RemoteAppends chan machineAppend

	// append requests (from local state machine)
	LocalAppends chan machineAppend

	// closing utilities
	closed chan struct{}
	closer chan struct{}
}

func newReplica(ctx common.Context, logger common.Logger, self peer, others []peer, parser Parser, stash stash.Stash) (*replica, error) {
	r := &replica{
		Ctx:             ctx,
		Id:              self.id,
		Self:            self,
		peers:           others,
		Logger:          logger,
		Parser:          parser,
		terms:           openTermStorage(stash),
		Log:             newEventLog(ctx),
		Replications:    make(chan replicateEvents),
		VoteRequests:    make(chan requestVote),
		RemoteAppends:   make(chan machineAppend),
		LocalAppends:    make(chan machineAppend),
		ElectionTimeout: time.Millisecond * time.Duration((rand.Intn(2000) + 1000)),
		RequestTimeout:  10 * time.Second,
		closed:          make(chan struct{}),
		closer:          make(chan struct{}, 1),
	}
	return r, r.start()
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

func (h *replica) start() error {

	// set the term from the durable store
	term, err := h.terms.Get(h.Self.id)
	if err != nil {
		return err
	}

	// set the term from durable storage.
	h.Term(term.num, term.leader, term.votedFor)

	go func() {
		l := h.Log.ListenCommits(0, 0)

		for i:=0;; i++{
			select {
			case <-h.closed:
				return
			case <-l.Closed():
				return
			case i:=<-l.Items():
				h.Logger.Info("Commit: %v", i)
			}
		}
	}()
	return nil

	// start snapshotter
	if h.Machine != nil {
		if err := h.startSnapshotter(); err != nil {
			return err
		}
	}

	return nil
}

func (h *replica) startSnapshotter() error {

	go func() {
		// l := h.Log.ListenCommits(0, 0)

		// logger := h.Logger.Fmt("Snapshotter: ")
		for {
			// var item LogItem
			// for i := 0; i < h.SnapshotThreshold; i++ {
			// select {
			// case <-h.closed:
			// return
			// case <-l.Closed():
			// return
			// case item = <-l.Items():
			// }
			// }
			//
			// snapshot, index, err := h.Machine.Snapshot()
			// if err != nil {
			// logger.Error("Error taking snapshot: %+v", err)
			// continue
			// }
			//
			// if err := h.Log.Compact(snapshot, index); err != nil {
			// logger.Error("Error compacting log: %+v", err)
			// continue
			// }
		}
	}()
	return nil
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

func (h *replica) Replicate(id uuid.UUID, term int, prevLogIndex int, prevLogTerm int, items []LogItem, commit int) (response, error) {
	append := replicateEvents{
		id, term, prevLogIndex, prevLogTerm, items, commit, make(chan response, 1)}

	select {
	case <-h.closed:
		return response{}, ClosedError
	case h.Replications <- append:
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
	append := machineAppend{event, make(chan machineAppendResponse, 1)}

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
type replicateEvents struct {
	id           uuid.UUID
	term         int
	prevLogIndex int
	prevLogTerm  int
	items        []LogItem
	commit       int
	ack          chan response
}

func (a replicateEvents) String() string {
	return fmt.Sprintf("Replicate(id=%v,prevIndex=%v,prevTerm=%v,commit=%v,items=%v)", a.id.String()[:8], a.prevLogIndex, a.prevLogTerm, a.commit, len(a.items))
}

func (a *replicateEvents) reply(term int, success bool) {
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
type machineAppend struct {
	Event Event
	ack   chan machineAppendResponse
}

func newLocalAppend(event Event) machineAppend {
	return machineAppend{event, make(chan machineAppendResponse, 1)}
}

func (a machineAppend) Return(item LogItem, err error) {
	a.ack <- machineAppendResponse{Item: item, Err: err}
}

func (a machineAppend) Fail(err error) {
	a.ack <- machineAppendResponse{Err: err}
}

// Client append request.  Requests are put onto the internal member
// channel and consumed by the currently active sub-machine.
//
// These come from active clients.
type machineAppendResponse struct {
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

type listener struct {
	raw *positionListener
	ch  chan LogItem
}

func newListener(raw *positionListener) *listener {
	return &listener{raw, make(chan LogItem)}
}

func (l *listener) start() {
	go func() {
		for {
			var i LogItem
			select {
			case <-l.raw.Closed():
				return
			case i = <-l.raw.Items():
			}

			select {
			case <-l.raw.Closed():
				return
			case l.ch <- i:
			}
		}
	}()
}

func (l *listener) Closed() <-chan struct{} {
	return l.raw.Closed()
}

func (l *listener) Items() <-chan LogItem {
	return l.ch
}

func (l *listener) Close() error {
	return l.raw.Close()
}

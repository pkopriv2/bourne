package kayak

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
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

	// the core database
	Db stash.Stash

	// data lock (currently using very coarse lock)
	lock sync.RWMutex

	// the current term.
	term term

	// the other peers. (currently static list)
	cluster     []peer
	clusterLock *sync.RWMutex

	// the current seq position
	seq concurrent.AtomicCounter

	// the election timeout.  (heartbeat: = timeout / 5)
	ElectionTimeout time.Duration

	// the client timeout
	RequestTimeout time.Duration

	// the event log.
	Log *eventLog

	// the durable term store.
	terms *termStore

	// request vote events.
	VoteRequests chan requestVote

	// append requests (presumably from leader)
	Replications chan replicateEvents

	// snapshot install (presumably from leader)
	Snapshots chan installSnapshot

	// append requests (from clients)
	RemoteAppends chan machineAppend

	// append requests (from local state machine)
	LocalAppends chan machineAppend

	// closing utilities
	closed chan struct{}
	closer chan struct{}
}

func newReplica(ctx common.Context, logger common.Logger, addr string, store LogStore, db *bolt.DB) (*replica, error) {
	termStore, err := openTermStorage(db)
	if err != nil {
		return nil, errors.Wrapf(err, "Host")
	}

	id, ok, err := termStore.GetId(addr)
	if err != nil {
		return nil, errors.Wrapf(err, "Host")
	}

	if !ok {
		id := uuid.NewV1()
		if err := termStore.SetId(addr, id); err != nil {
			return nil, errors.Wrapf(err, "Host")
		}
	}

	raw, err := store.Get(id)
	if err != nil {
		return nil, errors.Wrapf(err, "Host")
	}

	store.New(id, []byte{})


	log, err := openEventLog(logger, raw)
	if err != nil {
		return nil, errors.Wrapf(err, "Host")
	}


	r := &replica{
		Ctx:             ctx,
		Id:              id,
		Self:            peer{id, addr},
		Logger:          logger,
		terms:           termStore,
		Log:             log,
		Db:              db,
		Replications:    make(chan replicateEvents),
		VoteRequests:    make(chan requestVote),
		RemoteAppends:   make(chan machineAppend),
		LocalAppends:    make(chan machineAppend),
		Snapshots:       make(chan installSnapshot),
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
	return r.Log.Close()
}

func (h *replica) start() error {

	// retrieve the term from the durable store
	term, err := h.terms.Get(h.Self.id)
	if err != nil {
		return err
	}

	// set the term from durable storage.
	h.Term(term.Num, term.Leader, term.VotedFor)

	// retrieve the latest snapshot
	snapshot, err := h.Log.Snapshot()
	if err != nil {
		return errors.Wrapf(err, "Error retrieving latest snapshot for peer [%v]", h.Self.id)
	}

	// set the cluster from durable config.
	peers, err := snapshot.Config()
	if err != nil {
		return errors.Wrap(err, "error parsing config from snapshot")
	}

	h.SetCluster(peers)

	// start listening to the log for configuration changes.
	l, err := h.Log.ListenAppends(snapshot.PrevIndex+1, 256)
	if err != nil {
		return errors.Wrap(err, "Error registering listener")
	}

	go func() {
		defer l.Close()

		for i, e := l.Next(); e == nil; i, e = l.Next() {
			if i.Kind != Config {
				peers, err := parsePeers(i.Event, []peer{h.Self})
				if err != nil {
					h.Logger.Error("Error parsing configuration [%v]", peers)
					continue
				}

				h.SetCluster(peers)
			}
		}
	}()

	return nil
}

func (h *replica) String() string {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return fmt.Sprintf("%v, %v:", h.Self, h.term)
}

func (h *replica) Term(num int, leader *uuid.UUID, vote *uuid.UUID) error {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.term = term{num, leader, vote}
	h.Logger.Info("Durably storing updated term [%v]", h.term)
	return h.terms.Save(h.Id, h.term)
}

func (h *replica) CurrentTerm() term {
	h.lock.Lock()
	defer h.lock.Unlock()
	return h.term // i assume return is bound prior to the deferred function....
}

func (h *replica) Cluster() []peer {
	h.clusterLock.RLock()
	defer h.clusterLock.RUnlock()
	return append([]peer{}, h.cluster...)
}

func (h *replica) SetCluster(peers []peer) {
	h.clusterLock.Lock()
	defer h.clusterLock.Unlock()
	h.cluster = peers
}

func (h *replica) Peer(id uuid.UUID) (peer, bool) {
	for _, p := range h.Cluster() {
		if p.id == id {
			return p, true
		}
	}
	return peer{}, false
}

func (h *replica) Peers() []peer {
	cluster := h.Cluster()
	others := make([]peer, 0, len(cluster))
	for _, p := range cluster {
		if p.id != h.Self.id {
			others = append(others, p)
		}
	}
	return others
}

func (h *replica) Majority() int {
	return majority(len(h.Cluster()))
}

func (h *replica) Broadcast(fn func(c *client) response) <-chan response {
	peers := h.Peers()

	ret := make(chan response, len(peers))
	for _, p := range peers {
		go func(p peer) {
			cl, err := p.Client(h.Ctx)
			if cl == nil || err != nil {
				ret <- response{h.term.Num, false}
				return
			}

			defer cl.Close()
			ret <- fn(cl)
		}(p)
	}
	return ret
}

func (h *replica) InstallSnapshot(snapshotId uuid.UUID, id uuid.UUID, term int, prevLogIndex int, prevLogTerm int, prevConfig []byte, offset int, events []Event) (response, error) {
	req := installSnapshot{
		snapshotId, id, term, prevLogIndex, prevLogTerm, prevConfig, offset, events, make(chan response, 1)}

	select {
	case <-h.closed:
		return response{}, ClosedError
	case h.Snapshots <- req:
		select {
		case <-h.closed:
			return response{}, ClosedError
		case r := <-req.ack:
			return r, nil
		}
	}
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

func (h *replica) RemoteAppend(event Event, source uuid.UUID, seq int, kind int) (LogItem, error) {
	append := machineAppend{event, source, seq, kind, make(chan machineAppendResponse, 1)}

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

func (h *replica) LocalAppend(event Event, source uuid.UUID, seq int, kind int) (LogItem, error) {
	append := machineAppend{event, source, seq, kind, make(chan machineAppendResponse, 1)}

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

func (h *replica) Append(event Event, kind int) (LogItem, error) {
	seq := int(h.seq.Inc())

	for atmpt := 0; atmpt < 10; atmpt++ {
		item, err := h.LocalAppend(event, h.Self.id, seq, kind)
		if err == nil {
			return item, nil
		}
	}

	return LogItem{}, nil
}

func (h *replica) Listen(start int, buf int) (Listener, error) {
	return h.Log.ListenCommits(start, buf)
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
	maxLogIndex int
	maxLogTerm  int
	ack         chan response
}

func (r requestVote) String() string {
	return fmt.Sprintf("RequestVote(id=%v,idx=%v,term=%v)", r.id.String()[:8], r.maxLogIndex, r.maxLogTerm)
}

func (r requestVote) reply(term int, success bool) {
	r.ack <- response{term, success}
}

// Client append request.  Requests are put onto the internal member
// channel and consumed by the currently active sub-machine.
//
// These come from active clients.
type machineAppend struct {
	Event  Event
	Source uuid.UUID
	Seq    int
	Kind   int
	ack    chan machineAppendResponse
}

func (a machineAppend) Return(item LogItem, err error) {
	a.ack <- machineAppendResponse{Item: item, Err: err}
}

func (a machineAppend) Fail(err error) {
	a.ack <- machineAppendResponse{Err: err}
}

type installSnapshot struct {
	snapshotId   uuid.UUID
	leaderId     uuid.UUID
	term         int
	prevLogIndex int
	prevLogTerm  int
	prevConfig   []byte
	batchOffset  int
	batch        []Event
	ack          chan response
}

func (a installSnapshot) Reply(term int, success bool) {
	a.ack <- response{term, success}
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

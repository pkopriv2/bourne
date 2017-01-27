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
	uuid "github.com/satori/go.uuid"
)


type stdRequest struct {
	val interface{}
	reply chan interface{}
	err  chan error
}

func newStdRequest(val interface{}) stdRequest{
	return stdRequest{val, make(chan interface{}, 1), make(chan error, 1)}
}

func (r stdRequest) Body() interface{} {
	return r.val
}

func (r stdRequest) Reply(val interface{}) {
	r.reply<-val
}

func (r stdRequest) Fail(err error) {
	r.err<-err
}

func (r stdRequest)	Return(val interface{}, err error) {
	if err != nil {
		r.err<-err
	} else {
		r.reply<-val
	}
}

func (r stdRequest) Response() (interface{}, error) {
	select {
	case err := <-r.err:
		return nil, err
	case val := <-r.reply:
		return val, nil
	}
}

type stdResponse struct {
	Val interface{}
	Err error
}

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

	// the current cluster configuration
	Roster *roster

	// the core database
	Db *bolt.DB

	// data lock (currently using very coarse lock)
	lock sync.RWMutex

	// the current term.
	term term

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
	VoteRequests chan stdRequest

	// append requests (presumably from leader)
	Replications chan stdRequest

	// snapshot install (presumably from leader)
	Snapshots chan stdRequest

	// append requests (from clients)
	RemoteAppends chan stdRequest

	// append requests (from local state machine)
	LocalAppends chan stdRequest

	// append requests (from local state machine)
	RosterUpdates chan stdRequest

	// closing utilities
	closed chan struct{}
	closer chan struct{}
}

func newReplica(ctx common.Context, logger common.Logger, addr string, store LogStore, db *bolt.DB) (*replica, error) {
	termStore, err := openTermStore(db)
	if err != nil {
		return nil, errors.Wrapf(err, "Host")
	}

	id, ok, err := termStore.GetId(addr)
	if err != nil {
		return nil, errors.Wrapf(err, "Host")
	}

	if !ok {
		id = uuid.NewV1()
		if err := termStore.SetId(addr, id); err != nil {
			return nil, errors.Wrapf(err, "Host")
		}
	}

	self := peer{id, addr}
	logger = logger.Fmt("%v", self)
	logger.Info("Starting replica.")

	raw, err := store.Get(self.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Host")
	}

	if raw == nil {
		raw, err = store.New(self.Id, clusterBytes([]peer{}))
		if err != nil {
			return nil, errors.Wrapf(err, "Host")
		}
	}

	log, err := openEventLog(logger, raw)
	if err != nil {
		return nil, errors.Wrapf(err, "Host")
	}

	r := &replica{
		Ctx:             ctx,
		Id:              id,
		Self:            self,
		Logger:          logger,
		terms:           termStore,
		Log:             log,
		Db:              db,
		Roster:          newRoster([]peer{self}),
		Replications:    make(chan stdRequest),
		VoteRequests:    make(chan stdRequest),
		RemoteAppends:   make(chan stdRequest),
		LocalAppends:    make(chan stdRequest),
		Snapshots:       make(chan stdRequest),
		RosterUpdates:   make(chan stdRequest),
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
	term, err := h.terms.Get(h.Self.Id)
	if err != nil {
		return err
	}

	// set the term from durable storage.
	if err := h.Term(term.Num, term.Leader, term.VotedFor); err != nil {
		return err
	}

	// start the config loop
	go func() {
		for {
			// snapshot, err := h.Log.Snapshot()
			// if err != nil {
			// h.Logger.Error("Error getting snapshot: %+v", err)
			// return
			// }
			//
			// peers, err := parsePeers(snapshot.Config(), []peer{h.Self})
			// if err != nil {
			// h.Logger.Error("Error parsing config: %+v", err)
			// return
			// }
			//
			// h.Roster.Set(peers)

			l, err := h.Log.ListenAppends(0, 256)
			if err != nil {
				h.Logger.Error("Error starting listener: %+v", err)
				return
			}

			h.Logger.Info("Registered config listener.")
			for i, e := l.Next(); ; i, e = l.Next() {
				if e != nil {
					h.Logger.Error("Error parsing configuration [%v]", e)
					break
				}
				if i.Kind == Config {
					peers, err := parsePeers(i.Event, []peer{h.Self})
					if err != nil {
						h.Logger.Error("Error parsing configuration [%v]", peers)
						continue
					}

					h.Logger.Info("Appended [%v]", i, e)
					h.Roster.Set(peers)
				}
			}

			h.Logger.Error("Shutting down config manager [%v]", err)

			// if cause := errors.Cause(err); cause != OutOfBoundsError {
			// h.Logger.Error("Shutting down config manager [%v]", err)
			// return
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
	all, _ := h.Roster.Get()
	return all
}

func (h *replica) Leader() *peer {
	if term := h.CurrentTerm(); term.Leader != nil {
		peer, found := h.Peer(*term.Leader)
		if !found {
			return nil
			// panic(fmt.Sprintf("Unknown member [%v]: %v", term.Leader, h.Cluster()))
		} else {
			return &peer

		}
	}

	return nil
}

func (h *replica) Peer(id uuid.UUID) (peer, bool) {
	for _, p := range h.Cluster() {
		if p.Id == id {
			return p, true
		}
	}
	return peer{}, false
}

func (h *replica) Others() []peer {
	cluster := h.Cluster()
	others := make([]peer, 0, len(cluster))
	for _, p := range cluster {
		if p.Id != h.Self.Id {
			others = append(others, p)
		}
	}
	return others
}

func (h *replica) Majority() int {
	return majority(len(h.Cluster()))
}

func (h *replica) Broadcast(fn func(c *rpcClient) response) <-chan response {
	peers := h.Others()
	h.Logger.Info("Broadcasting heartbeat: %v", peers)

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

func (h *replica) sendRequest(ch chan<- stdRequest, val interface{}) (interface{}, error) {
	timer := time.NewTimer(h.RequestTimeout)
	req := newStdRequest(val)
	select {
	case <-h.closed:
		return nil, ClosedError
	case <-timer.C:
		return nil, common.NewTimeoutError(h.RequestTimeout, "ClientAppend")
	case ch <- req:
		select {
		case <-h.closed:
			return nil, ClosedError
		case r := <-req.reply:
			return r, nil
		case e := <-req.err:
			return nil, e
		case <-timer.C:
			return nil, common.NewTimeoutError(h.RequestTimeout, "ClientAppend")
		}
	}
}

func (h *replica) UpdateRoster(update rosterUpdate) error {
	_, err := h.sendRequest(h.RosterUpdates, update)
	return err
}

func (h *replica) AddPeer(peer peer) error {
	return h.UpdateRoster(rosterUpdate{peer, true})
}

func (h *replica) DelPeer(peer peer) error {
	return h.UpdateRoster(rosterUpdate{peer, false})
}

func (h *replica) InstallSnapshot(snapshot installSnapshot) (response, error) {
	val, err := h.sendRequest(h.Snapshots, snapshot)
	if err != nil {
		return response{}, err
	}
	return val.(response), nil
}

func (h *replica) Replicate(replicate replicateEvents) (response, error) {
	val, err := h.sendRequest(h.Replications, replicate)
	if err != nil {
		return response{}, err
	}
	return val.(response), nil
}

func (h *replica) RequestVote(vote requestVote) (response, error) {
	val, err := h.sendRequest(h.VoteRequests, vote)
	if err != nil {
		return response{}, err
	}
	return val.(response), nil
}

func (h *replica) RemoteAppend(append appendEvent) (LogItem, error) {
	val, err := h.sendRequest(h.RemoteAppends, append)
	if err != nil {
		return LogItem{}, err
	}
	return val.(LogItem), nil
}

func (h *replica) LocalAppend(append appendEvent) (LogItem, error) {
	val, err := h.sendRequest(h.LocalAppends, append)
	if err != nil {
		return LogItem{}, err
	}
	return val.(LogItem), nil
}

func (h *replica) Append(event Event, kind int) (LogItem, error) {
	return h.LocalAppend(appendEvent{event, h.Self.Id, 0, kind})
}

func (h *replica) Listen(start int, buf int) (Listener, error) {
	return h.Log.ListenCommits(start, buf)
}

func majority(num int) int {
	return int(math.Ceil(float64(num) / float64(2)))
}


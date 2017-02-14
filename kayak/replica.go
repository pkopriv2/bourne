package kayak

import (
	"fmt"
	"io"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
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

	// the control (de-normalized from Ctx.Logger())
	logger common.Logger

	// the control (de-normalized from Ctx.Control())
	ctrl common.Control

	// the unique id of this member. (copied for brevity from self)
	Id uuid.UUID

	// the peer representing the local instance
	Self Peer

	// the networking abstraction
	Network net.Network

	// the core database
	Db *bolt.DB

	// the current cluster configuration
	Roster *roster

	// the event log.
	Log *eventLog

	// data lock (currently using very coarse lock)
	lock sync.RWMutex

	// the current term.
	term term

	// the durable term store.
	terms *termStore

	// the election timeout.  (heartbeat: = timeout / 5)
	ElectionTimeout time.Duration

	// the client timeout
	RequestTimeout time.Duration

	// the connection timeout
	ConnTimeout time.Duration

	// read barrier request
	Barrier chan *common.Request

	// request vote events.
	VoteRequests chan *common.Request

	// append requests (presumably from leader)
	Replications chan *common.Request

	// snapshot install (presumably from leader)
	Snapshots chan *common.Request

	// append requests (from clients)
	RemoteAppends chan *common.Request

	// append requests (from local state machine)
	LocalAppends chan *common.Request

	// append requests (from local state machine)
	RosterUpdates chan *common.Request
}

func newReplica(ctx common.Context, net net.Network, store LogStore, db *bolt.DB, addr string) (*replica, error) {

	termStore, err := openTermStore(db)
	if err != nil {
		return nil, errors.Wrapf(err, "Error retrieving term store")
	}

	id, err := getOrCreateReplicaId(termStore, addr)
	if err != nil {
		return nil, errors.Wrapf(err, "Error retrieving peer id [%v]", addr)
	}

	self := Peer{id, addr}
	ctx = ctx.Sub("%v", self)
	ctx.Logger().Info("Starting replica.")

	log, err := getOrCreateStore(ctx, store, self)
	if err != nil {
		return nil, errors.Wrapf(err, "Error retrieving durable log [%v]", id)
	}
	ctx.Control().Defer(func(cause error) {
		log.Close()
	})

	roster := newRoster([]Peer{self})
	ctx.Control().Defer(func(cause error) {
		roster.Close()
	})

	ctx.Control().Defer(func(cause error) {
		ctx.Logger().Info("Replica closed: %v", cause)
	})

	baseElectionTimeout := ctx.Config().OptionalDuration(Config.BaseElectionTimeout, Config.BaseElectionTimeoutDefault)
	rndmElectionTimeout := time.Duration(int64(rand.Intn(1000000000)))
	r := &replica{
		Ctx:             ctx,
		logger:          ctx.Logger(),
		ctrl:            ctx.Control(),
		Id:              id,
		Self:            self,
		terms:           termStore,
		Log:             log,
		Db:              db,
		Roster:          roster,
		Network:         net,
		Barrier:         make(chan *common.Request),
		Replications:    make(chan *common.Request),
		VoteRequests:    make(chan *common.Request),
		RemoteAppends:   make(chan *common.Request),
		LocalAppends:    make(chan *common.Request),
		Snapshots:       make(chan *common.Request),
		RosterUpdates:   make(chan *common.Request),
		ElectionTimeout: baseElectionTimeout + rndmElectionTimeout,
		RequestTimeout:  ctx.Config().OptionalDuration(Config.RequestTimeout, Config.RequestTimeoutDefault),
		ConnTimeout:     ctx.Config().OptionalDuration(Config.ConnectionTimeout, Config.ConnectionTimeoutDefault),
	}
	return r, r.start()
}

func (h *replica) Close() error {
	return h.ctrl.Close()
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

	listenRosterChanges(h)
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
	h.logger.Info("Durably storing updated term [%v]", h.term)
	return h.terms.Save(h.Id, h.term)
}

func (h *replica) CurrentTerm() term {
	h.lock.Lock()
	defer h.lock.Unlock()
	return h.term // i assume return is bound prior to the deferred function....
}

func (h *replica) Cluster() []Peer {
	all, _ := h.Roster.Get()
	return all
}

func (h *replica) Leader() *Peer {
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

func (h *replica) Peer(id uuid.UUID) (Peer, bool) {
	for _, p := range h.Cluster() {
		if p.Id == id {
			return p, true
		}
	}
	return Peer{}, false
}

func (h *replica) Others() []Peer {
	cluster := h.Cluster()
	others := make([]Peer, 0, len(cluster))
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
	ret := make(chan response, len(peers))
	for _, p := range peers {
		go func(p Peer) {
			cl, err := p.Client(h.Ctx, h.Network, h.ConnTimeout)
			if cl == nil || err != nil {
				ret <- newResponse(h.term.Num, false)
				return
			}

			defer cl.Close()
			ret <- fn(cl)
		}(p)
	}
	return ret
}

func (h *replica) sendRequest(ch chan<- *common.Request, timeout time.Duration, val interface{}) (interface{}, error) {
	timer := time.NewTimer(timeout)

	req := common.NewRequest(val)
	defer req.Cancel()

	select {
	case <-h.ctrl.Closed():
		return nil, errors.WithStack(common.ClosedError)
	case <-timer.C:
		return nil, errors.Wrapf(common.TimeoutError, "Request timed out waiting for machine to accept [%v]", timeout)
	case ch <- req:
		select {
		case <-h.ctrl.Closed():
			return nil, errors.WithStack(common.ClosedError)
		case r := <-req.Acked():
			return r, nil
		case e := <-req.Failed():
			return nil, errors.Wrap(e, "Request failed")
		case <-timer.C:
			return nil, errors.Wrapf(common.TimeoutError, "Request timed out waiting for machine to response [%v]", timeout)
		}
	}
}

func (h *replica) AddPeer(peer Peer) error {
	return h.UpdateRoster(rosterUpdate{peer, true})
}

func (h *replica) DelPeer(peer Peer) error {
	return h.UpdateRoster(rosterUpdate{peer, false})
}

func (h *replica) Append(event Event, kind Kind) (Entry, error) {
	return h.LocalAppend(appendEvent{event, kind})
}

func (h *replica) Listen(start int, buf int) (Listener, error) {
	return h.Log.ListenCommits(start, buf)
}

func (h *replica) Compact(until int, data <-chan Event, size int) error {
	return h.Log.Compact(until, data, size, clusterBytes(h.Cluster()))
}

func (h *replica) UpdateRoster(update rosterUpdate) error {
	_, err := h.sendRequest(h.RosterUpdates, 30*time.Second, update)
	return err
}

func (h *replica) ReadBarrier() (int, error) {
	val, err := h.sendRequest(h.Barrier, h.RequestTimeout, nil)
	if err != nil {
		return 0, err
	}
	return val.(int), nil
}

func (h *replica) InstallSnapshot(snapshot installSnapshot) (response, error) {
	val, err := h.sendRequest(h.Snapshots, h.RequestTimeout, snapshot)
	if err != nil {
		return response{}, err
	}
	return val.(response), nil
}

func (h *replica) Replicate(r replicate) (response, error) {
	val, err := h.sendRequest(h.Replications, h.RequestTimeout, r)
	if err != nil {
		return response{}, err
	}
	return val.(response), nil
}

func (h *replica) RequestVote(vote requestVote) (response, error) {
	val, err := h.sendRequest(h.VoteRequests, h.RequestTimeout, vote)
	if err != nil {
		return response{}, err
	}
	return val.(response), nil
}

func (h *replica) RemoteAppend(append appendEvent) (Entry, error) {
	val, err := h.sendRequest(h.RemoteAppends, h.RequestTimeout, append)
	if err != nil {
		return Entry{}, err
	}
	return val.(Entry), nil
}

func (h *replica) LocalAppend(append appendEvent) (Entry, error) {
	val, err := h.sendRequest(h.LocalAppends, h.RequestTimeout, append)
	if err != nil {
		return Entry{}, err
	}
	return val.(Entry), nil
}

func majority(num int) int {
	if num%2 == 0 {
		return 1 + (num / 2)
	} else {
		return int(math.Ceil(float64(num) / float64(2)))
	}
}

func newLeaderPool(self *replica, size int) common.ObjectPool {
	return common.NewObjectPool(self.Ctx.Control().Sub(), size, newLeaderConstructor(self))
}

func newLeaderConstructor(self *replica) func() (io.Closer, error) {
	return func() (io.Closer, error) {
		var cl *rpcClient
		for cl == nil {
			leader := self.Leader()
			if leader == nil {
				time.Sleep(self.ElectionTimeout)
				continue
			}

			cl, _ = leader.Client(self.Ctx, self.Network, self.ConnTimeout)
		}
		return cl, nil
	}
}

func getOrCreateReplicaId(store *termStore, addr string) (uuid.UUID, error) {
	id, ok, err := store.GetId(addr)
	if err != nil {
		return uuid.UUID{}, errors.Wrapf(err, "Error retrieving id for address [%v]", addr)
	}

	if !ok {
		id = uuid.NewV1()
		if err := store.SetId(addr, id); err != nil {
			return uuid.UUID{}, errors.Wrapf(err, "Error associating addr [%v] with id [%v]", addr, id)
		}
	}

	return id, nil
}

func getOrCreateStore(ctx common.Context, store LogStore, self Peer) (*eventLog, error) {
	raw, err := store.Get(self.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Error opening stored log [%v]", self.Id)
	}

	if raw == nil {
		raw, err = store.New(self.Id, clusterBytes([]Peer{self}))
		if err != nil {
			return nil, errors.Wrapf(err, "Error opening stored log [%v]", self.Id)
		}
	}

	return openEventLog(ctx, raw)
}

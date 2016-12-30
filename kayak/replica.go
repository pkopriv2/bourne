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
	ctx common.Context

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

	// the distributed event log.
	Log *eventLog

	// the durable term store.
	terms *storage

	// A channel whose elements are the ordered events as they are committed.
	Committed chan event

	// request vote events.
	Votes chan requestVote

	// append requests (presumably from leader)
	Appends chan appendEvents

	// append requests (from clients)
	ClientAppends chan clientAppend
}

func newReplica(ctx common.Context, logger common.Logger, self peer, others []peer, parser Parser, terms *storage) (*replica, error) {
	term, err := terms.GetTerm(self.id)
	if err != nil {
		return nil, err
	}

	return &replica{
		ctx:             ctx,
		Id:              self.id,
		Self:            self,
		peers:           others,
		Logger:          logger,
		Parser:          parser,
		term:            term,
		terms:           terms,
		Log:             newEventLog(ctx),
		Appends:         make(chan appendEvents),
		Votes:           make(chan requestVote),
		ClientAppends:   make(chan clientAppend),
		ElectionTimeout: time.Millisecond * time.Duration((rand.Intn(1000) + 1000)),
		RequestTimeout:  2 * time.Second,
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

	h.terms.PutTerm(h.Id, h.term)
	// select {
	// default:
	// // case h.Terms<-h.term:
	// }
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
	ret := make([]peer, 0, len(h.peers)+1)
	ret = append(ret, h.Peers()...)
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
			cl, err := p.Client(h.ctx, h.Parser)
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

func majority(num int) int {
	return int(math.Ceil(float64(num) / float64(2)))
}

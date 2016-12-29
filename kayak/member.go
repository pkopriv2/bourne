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
	var leaderStr string
	if t.leader == nil {
		leaderStr = "nil"
	} else {
		leaderStr = t.leader.String()[:8]
	}

	var votedForStr string
	if t.votedFor == nil {
		votedForStr = "nil"
	} else {
		votedForStr = t.votedFor.String()[:8]
	}

	return fmt.Sprintf("(%v,%v,%v)", t.num, leaderStr, votedForStr)
}

// The member is the primary identity within a cluster.  Within the core machine,
// only a single instance ever exists, but its location within the machine
// may change over time.  Therefore all updates/requests must be forwarded
// to the machine currently processing the member.
type member struct {

	// configuration used to build this instance.
	ctx common.Context

	// the core member logger
	logger common.Logger

	// the unique id of this member.
	id uuid.UUID

	// the peer representing the local instance
	self peer

	// the event parser. (used to spawn clients.)
	parser Parser

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
	log *eventLog

	// A channel whose elements are the ordered events as they committed.
	committed chan event

	// request vote events.
	votes chan requestVote

	// append requests (from peers)
	appends chan appendEvents

	// append requests (from clients)
	clientAppends chan clientAppend

	// consumers
	terms chan term
}

func newMember(ctx common.Context, logger common.Logger, self peer, others []peer, parser Parser) *member {
	return &member{
		ctx:             ctx,
		id:              self.id,
		self:            self,
		peers:           others,
		logger:          logger,
		parser:          parser,
		log:             newEventLog(ctx),
		appends:         make(chan appendEvents),
		votes:           make(chan requestVote),
		clientAppends:   make(chan clientAppend),
		ElectionTimeout: time.Millisecond * time.Duration((rand.Intn(1000) + 1000)),
		RequestTimeout:  10 * time.Second,
	}
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

	select {
	default:
	case h.terms <- h.term:
	}
}

func (h *member) CurrentTerm() term {
	h.lock.Lock()
	defer h.lock.Unlock()
	return h.term // i assume return is bound prior to the deferred function....
}

func (h *member) Peer(id uuid.UUID) (peer, bool) {
	for _, p := range h.Peers() {
		if p.id == id {
			return p, true
		}
	}
	return peer{}, false
}

func (h *member) Peers() []peer {
	h.lock.RLock()
	defer h.lock.RUnlock()
	ret := make([]peer, 0, len(h.peers))
	return append(ret, h.peers...)
}

func (h *member) Majority() int {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return majority(len(h.peers) + 1)
}

func (h *member) Broadcast(fn func(c *client) response) <-chan response {
	peers := h.Peers()

	ret := make(chan response, len(peers))
	for _, p := range peers {
		go func(p peer) {
			cl, err := p.Client(h.ctx, h.parser)
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

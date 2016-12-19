package kayak

import (
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/convoy"
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
//	* better lifecycle semantics.  currently very difficult to reason about state expectations.
//    this may mean documentation.
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

func (r requestVote) reply(term int, success bool) bool {
	select {
	case r.ack <- response{term, success}:
		return true
	default:
		return false // shouldn't be possible
	}
}

type appendEvents struct {
	id           uuid.UUID
	term         int
	prevLogIndex int
	prevLogTerm  int
	events       []event
	commit       int
	ack          chan response
}

func (a *appendEvents) reply(term int, success bool) bool {
	select {
	case a.ack <- response{term, success}:
		return true
	default:
		return false // shouldn't be possible
	}
}

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

type peer struct {
	raw  convoy.Member
	port int
}

func (p peer) Client(ctx common.Context) (*client, error) {
	conn, err := p.raw.Connect(p.port)
	if err != nil {
		return nil, err
	}

	clnt, err := net.NewClient(ctx,
		ctx.Logger().Fmt("Client(%v,%v):", p.raw.Id(), p.port), conn)
	if err != nil {
		return nil, err
	}

	return &client{clnt}, nil
}

// snapshot of all mutable state.  (deep copied)
type snapshot struct {
	term        int
	leader      *uuid.UUID
	votedFor    *uuid.UUID
	peers       []peer
	offsets     map[uuid.UUID]int
	commits     map[uuid.UUID]int
	maxLogIndex int
	maxLogTerm  int
	commit      int
}

type member struct {

	// the unique id of this member.
	id uuid.UUID

	// the main context
	ctx common.Context

	// the root logger
	logger common.Logger

	// data lock (currently using very coarse lock)
	lock sync.Mutex

	// the current term information. (as seen by this member)
	term int

	// the current term information. (as seen by this member)
	leader *uuid.UUID

	// who was voted for this term
	votedFor *uuid.UUID

	// the raw membership member
	raw convoy.Host

	// the election timeout.  randomized between 500 and 1000 ms
	timeout time.Duration

	// request vote events.
	votes chan requestVote

	// append requests
	appends chan appendEvents

	// tracks follower states when leader
	offsets map[uuid.UUID]int
	commits map[uuid.UUID]int

	// the other peers. (currently static list)
	peers []peer

	// the distributed event log.
	log *eventLog

	// closing utilities.
	closed chan struct{}
	closer chan struct{}
}

func newMember(ctx common.Context, self convoy.Host, others []convoy.Member) (*member, error) {
	peers := make([]peer, 0, len(others))
	for _, p := range others {
		peers = append(peers, peer{raw: p})
	}

	m := &member{
		id:      self.Id(),
		peers:   peers,
		raw:     self,
		log:     newViewLog(ctx),
		appends: make(chan appendEvents),
		votes:   make(chan requestVote),
		timeout: time.Millisecond * time.Duration((rand.Intn(500) + 500)),
	}

	if err := m.start(); err != nil {
		return nil, err
	}

	return m, nil
}

func (h *member) start() error {
	h.becomeFollower(nil, 0, nil)
	return nil
}

// in the spirit of raft, I think this adds to understandability through safety.
// NOTE: used only on external reads.
func (h *member) snapshot() snapshot {
	h.lock.Lock()
	defer h.lock.Unlock()

	peers := make([]peer, 0, len(h.peers))
	for _, p := range h.peers {
		peers = append(peers, p)
	}

	maxLogIndex, maxLogTerm, commit := h.log.Snapshot()
	return snapshot{
		votedFor:    h.votedFor,
		leader:      h.leader,
		maxLogIndex: maxLogIndex,
		maxLogTerm:  maxLogTerm,
		peers:       peers,
		term:        h.term,
		commit:      commit,
	}
}

func (h *member) setTerm(id *uuid.UUID, term int, vote *uuid.UUID) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.leader = id
	h.term = term
	h.votedFor = vote
	h.commits = make(map[uuid.UUID]int)
	h.offsets = make(map[uuid.UUID]int)
}

func (h *member) castVote(id uuid.UUID) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.votedFor = &id
}

func (h *member) RequestAppendEvents(id uuid.UUID, term int, logIndex int, logTerm int, batch []event, commit int) (response, error) {
	append := appendEvents{
		id, term, logIndex, logTerm, batch, commit, make(chan response, 1)}

	select {
	case <-h.closed:
		return response{}, ClosedError
	case h.appends <- append:
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
	select {
	case <-h.closed:
		return response{}, ClosedError
	case h.votes <- req:
		select {
		case <-h.closed:
			return response{}, ClosedError
		case r := <-req.ack:
			return r, nil
		}
	}
}

func (h *member) becomeCandidate() {
	// increment term and vote forself.
	h.setTerm(nil, h.term+1, &h.id)

	// decorate the logger
	logger := h.logger.Fmt("Candidate[%v]", h.term)

	// kick off candidate routine
	go func() {
		defer logger.Info("No longer candidate.")

		// send out ballots
		ballots := h.requestVote()

		// start the timer
		timer := time.NewTimer(h.timeout)

		var append appendEvents
		var vote requestVote
		var numVotes int = 0
		for {
			needed := majority(len(h.peers))
			if numVotes >= needed {
				h.becomeLeader()
				return
			}

			select {
			case <-h.closed:
				return
			case append = <-h.appends:

				if append.term < h.term {
					append.reply(h.term, false)
					continue
				}

				// append.term is >= term.  use it from now on.
				append.reply(append.term, false)
				h.becomeFollower(&append.id, append.term, &append.id)
				return

			case vote = <-h.votes:

				if vote.term <= h.term {
					vote.reply(h.term, false)
					continue
				}

				h.becomeFollower(nil, vote.term, &vote.id)
				return

			case <-timer.C:
				h.becomeCandidate()
				return
			case vote := <-ballots:

				if vote.term > h.term {
					h.becomeFollower(nil, vote.term, nil)
					return
				}

				if vote.success {
					numVotes++
				}
			}
		}
	}()
}

func (h *member) becomeLeader() {
	logger := h.logger.Fmt("Leader[%v]", h.term)
	logger.Info("Becoming leader")

	// set self as leader.
	h.setTerm(&h.id, h.term, nil)

	// start leader routine
	go func() {
		defer logger.Info("No longer leader.")

		// handles append requests while candidate
		var append appendEvents
		var vote requestVote

		// start the timer
		timer := time.NewTimer(h.timeout / 3)

		for {
			select {
			case <-h.closed:
				return
			case append = <-h.appends:

				if append.term <= h.term {
					append.reply(h.term, false)
					continue
				}

				append.reply(h.term, false)
				h.becomeFollower(&append.id, append.term, &append.id)
				return

			case vote = <-h.votes:

				if vote.term <= h.term {
					vote.reply(h.term, false)
					continue
				}

				// Only vote for candidates with logs at least as new as ours, but
				// either way we're done being a leader.
				maxLogIndex, maxLogTerm, _ := h.log.Snapshot()
				if vote.maxLogIndex >= maxLogIndex && vote.maxLogTerm >= maxLogTerm {
					vote.reply(vote.term, true)
				} else {
					vote.reply(vote.term, false)
				}

				h.becomeFollower(nil, vote.term, &vote.id)
				return
			case <-timer.C:
				needed := majority(len(h.peers))

				ch := h.heartBeat()
				for i := 0; i < needed; i++ {
					resp := <-ch
					if resp.term > h.term {
						h.becomeFollower(nil, resp.term, nil)
						return
					}
				}
				return
			}
		}
	}()
}

func (h *member) becomeFollower(id *uuid.UUID, term int, vote *uuid.UUID) {
	logger := h.logger.Fmt("Follower[%v, %v]", term, id)
	logger.Info("Becoming follower")

	h.setTerm(id, term, vote)

	go func() {
		defer logger.Info("No longer follower.")

		// handles append requests while candidate
		var append appendEvents
		var vote requestVote

		// start the timer
		timer := time.NewTimer(h.timeout)

		for {
			select {
			case <-h.closed:
				return
			case append = <-h.appends:
				if append.term < term {
					append.reply(term, false)
					continue
				}

				if append.term > term {
					append.reply(term, false)
					h.becomeFollower(&append.id, append.term, &append.id)
					return
				}

				logTerm, _ := h.log.Get(append.prevLogIndex)
				if logTerm != append.prevLogTerm {
					append.reply(term, false)
					continue
				}

				h.log.Append(append.events, append.prevLogIndex+1, term)
				return

			case vote = <-h.votes:
				if vote.term < term {
					vote.reply(term, false)
					continue
				}

				// Only accept candidates with logs at least as new as ours.
				maxLogIndex, maxLogTerm, _ := h.log.Snapshot()
				if vote.maxLogIndex < maxLogIndex || vote.maxLogTerm < maxLogTerm {
					vote.reply(vote.term, false)
					continue
				}

				if h.votedFor != nil {
					vote.reply(term, false)
					continue
				}

				vote.reply(term, true)
				if vote.term != term {
					h.becomeFollower(nil, vote.term, &vote.id)
					return
				}

			case <-timer.C:
				h.becomeCandidate()
				return
			}
		}
	}()
}

// expects stable internal state
func (h *member) requestVote() <-chan response {
	ch := make(chan response, len(h.peers))

	for _, p := range h.peers {
		h.logger.Info("Sending ballots to peer [%v]", p)
		go func(p peer) {
			client, err := p.Client(h.ctx)
			if err != nil {
				ch <- response{h.term, false}
				return
			}

			maxLogIndex, maxLogTerm, _ := h.log.Snapshot()
			resp, err := client.RequestVote(h.id, h.term, maxLogIndex, maxLogTerm)
			if err != nil {
				ch <- response{h.term, false}
				return
			}

			ch <- resp
		}(p)
	}

	return ch
}

// expects stable internal state
func (h *member) heartBeat() <-chan response {
	// maxLogIndex, maxLogTerm, commit := h.log.Snapshot()

	ch := make(chan response, len(h.peers))
	for _, p := range h.peers {
		h.logger.Debug("Sending heartbeats to peer [%v]", p)

		go func(p peer) {
			client, err := p.Client(h.ctx)
			if err != nil {
				ch <- response{h.term, false}
				return
			}

			resp, err := client.AppendEvents(h.id, h.term, 0, 0, []event{}, 0)
			if err != nil {
				ch <- response{h.term, false}
				return
			}

			ch <- resp
		}(p)

	}

	return ch
}

func majority(num int) int {
	return int(math.Ceil(float64(num) / float64(2)))
}

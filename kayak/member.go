package kayak

import (
	"math"
	"sync"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/convoy"
	"github.com/pkopriv2/bourne/net"
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

type requestVote struct {
	id            uuid.UUID
	term          int
	lastLogTerm   int
	lastLogOffset int
	ack           chan<- response
}

func (r *requestVote) reply(term int, success bool) bool {
	select {
	case r.ack <- response{term, success}:
		return true
	default:
		return false // shouldn't be possible
	}
}

type appendEvents struct {
	id            uuid.UUID
	term          int
	events        []event
	prevLogOffset int
	prevLogTerm   int
	commit        int
	ack           chan<- response
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

type peer struct {
	id  uuid.UUID
	raw convoy.Member

	// transient...cached for efficiency
	client *client
}

type term struct {
	num    int
	leader uuid.UUID
}

type host struct {

	// the unique id of this host.
	id uuid.UUID

	//
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

	// the raw membership host
	raw convoy.Host

	// the number of workers the host allows.
	workers int

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

func (h *host) start() error {
	return nil
}

func (h *host) currentTerm() (leader *uuid.UUID, term int, vote *uuid.UUID) {
	h.lock.Lock()
	defer h.lock.Unlock()
	leader = h.leader
	term = h.term
	vote = h.votedFor
	return
}

func (h *host) setTerm(id *uuid.UUID, term int, vote *uuid.UUID) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.leader = id
	h.term = term
	h.votedFor = vote
	h.commits = make(map[uuid.UUID]int)
	h.offsets = make(map[uuid.UUID]int)
}

func (h *host) castVote(id uuid.UUID) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.votedFor = &id
}


func (h *host) becomeCandidate() {

	_, term, _ := h.currentTerm()

	// increment term
	term++

	// set term and reset leader
	h.setTerm(nil, term, &h.id)

	// decorate the logger
	logger := h.logger.Fmt("Candidate[%v]", term)

	// number of votes to constitute a majority
	needed := majority(len(h.peers))

	// snapshot the log
	maxIndex, maxTerm := h.log.Max()

	// kick off candidate routine
	go func() {
		defer logger.Info("No longer candidate.")

		ballots := make(chan response, len(h.peers))
		for _, p := range h.peers {
			logger.Info("Sending ballots to peer [%v]", p)
			go func(p peer) {
				resp, err := p.client.RequestVote(h.id, term, maxIndex, maxTerm)
				if err != nil {
					ballots <- response{term, false}
					return
				}

				ballots <- resp
			}(p)
		}

		// handles append requests while candidate
		var append appendEvents
		var appendOk bool
		var vote requestVote
		var voteOk bool

		// start the timer
		timer := time.NewTimer(h.timeout)

		// track ayes
		var numVotes int = 0
		for {
			if numVotes >= needed {
				h.becomeLeader()
				return
			}

			select {
			case append, appendOk = <-h.appends:
				if !appendOk {
					return
				}

				append.reply(term, false)
				h.becomeFollower(&append.id, append.term, &append.id)
				return

			case vote, voteOk = <-h.votes:
				if !voteOk {
					return
				}

				if vote.term <= term {
					vote.reply(term, false)
					continue
				}

				h.becomeFollower(nil, vote.term, &vote.id)
				return

			case <-timer.C:
				h.becomeCandidate()
				return
			case vote := <-ballots:

				if vote.term > term {
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

func (h *host) becomeLeader() {
}

func (h *host) becomeFollower(id *uuid.UUID, term int, vote *uuid.UUID) {
	logger := h.logger.Fmt("Follower[%v, %v]", term, id)

	h.setTerm(id, term, vote)

	go func() {
		defer logger.Info("No longer follower.")

		// handles append requests while candidate
		var append appendEvents
		var appendOk bool
		var vote requestVote
		var voteOk bool

		// start the timer
		timer := time.NewTimer(h.timeout)

		for {
			select {
			case append, appendOk = <-h.appends:
				if !appendOk {
					return
				}


				if append.term > term {
					h.becomeFollower(&append.id, append.term, &append.id)
					return
				}

				if append.term > term {
					h.becomeFollower(&append.id, append.term, &append.id)
					return
				}

				if append.term < term {
					append.reply(term, false)
					return
				}

				logTerm, _ := h.log.Get(append.prevLogOffset)
				if logTerm != append.prevLogTerm {
					append.reply(term, false)
					continue
				}

				h.log.Append(append.events, append.prevLogOffset+1, term)
				return

			case vote, voteOk = <-h.votes:
				if !voteOk {
					return
				}

				if vote.term < term {
					vote.reply(term, false)
					continue
				}

				return

			case <-timer.C:
				h.becomeCandidate()
				return
			}
		}
	}()

}

type event interface {
}

type client struct {
	raw net.Connection
}

func (c *client) AppendEvents(id uuid.UUID, commit int, term int, logIndex int, logTerm int, batch []event) error {
	panic("")
}

func (c *client) RequestVote(id uuid.UUID, term int, logIndex int, logTerm int) (response, error) {
	panic("")
}

func majority(num int) int {
	return int(math.Ceil(float64(num) / float64(2)))
}

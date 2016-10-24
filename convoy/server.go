package convoy

import (
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
)

type Server interface {
	Serve(chan<- interface{}, interface{})
}

type server struct {
	ctx  common.Context
	pool concurrent.WorkPool
	peer Peer

	timeoutPing   time.Duration
	timeoutUpdate time.Duration
}

func (s *server) Serve(res chan<- interface{}, raw interface{}) {
	err := s.pool.Submit(func() {
		switch req := raw.(type) {
		case PingRequest:
			s.handlePing(req, res)
			return
		case ProxyPingRequest:
			s.handleProxyPing(req, res)
			return
		case UpdateRequest:
			s.handleUpdate(req, res)
			return
		}
	})

	if err != nil {
		res <- ErrorResponse{err}
	}
}

func (s *server) handlePing(req PingRequest, res chan<- interface{}) {
	res <- PingResponse{}
}

func (s *server) handleProxyPing(req ProxyPingRequest, res chan<- interface{}) {
	member := s.peer.Roster().Get(req.Target)
	if member == nil {
		res <- ProxyPingResponse{false, NoSuchMemberError{req.Target}}
		return
	}

	client, err := member.client()
	if err != nil {
		s.peer.update([]update{newLeave(member.Id(), member.Version())})
		res <- ProxyPingResponse{false, err}
		return
	}

	success, err := client.Ping(s.timeoutPing)
	res <- ProxyPingResponse{success, err}
}

func (s *server) handleUpdate(req UpdateRequest, res chan<- interface{}) {
	res <- UpdateResponse{s.peer.update(req.Updates)}
}

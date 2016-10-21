package convoy

import (
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
)

const (
	confPingTimeout   = "convoy.ping.timeout"
	confUpdateTimeout = "convoy.update.timeout"
)

const (
	defaultPingTimeout   = time.Second
	defaultUpdateTimeout = time.Second
)

type Server interface {
	Serve(chan<- interface{}, interface{})
}

type server struct {
	ctx    common.Context
	pool   concurrent.WorkPool
	pinger Pinger
	peer   Peer

	timeoutPing   time.Duration
	timeoutUpdate time.Duration
}

func (s *server) Serve(res chan<- interface{}, raw interface{}) {
	err := s.pool.Submit(res, func(resp concurrent.Response) {
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
		res <- ErrorResponse{}
	}
}

func (s *server) handlePing(req PingRequest, res concurrent.Response) {
	res <- PingResponse{}
}

func (s *server) handleProxyPing(req ProxyPingRequest, res concurrent.Response) {
	success, err := s.peer.Ping(req.Target)
	res <- ProxyPingResponse{success, err}
}

func (s *server) handleUpdate(req UpdateRequest, res concurrent.Response) {
	res <- UpdateResponse{s.peer.Update(req.Update)}
}

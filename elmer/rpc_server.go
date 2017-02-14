package elmer

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
)

type server struct {
	ctx    common.Context
	logger common.Logger
	self   *indexer
}

// Returns a new service handler for the ractlica
func newServer(ctx common.Context, listener net.Listener, self *indexer, workers int) (net.Server, error) {
	ctx = ctx.Sub("Rpc")

	server := &server{
		ctx:    ctx,
		logger: ctx.Logger(),
		self:   self}

	return net.NewServer(ctx, listener, serverInitHandler(server), workers)
}

func serverInitHandler(s *server) func(net.Request) net.Response {
	return func(req net.Request) net.Response {
		action, err := readMeta(req.Meta())
		if err != nil {
			return net.NewErrorResponse(errors.Wrap(err, "Error parsing action"))
		}

		switch action {
		default:
			return net.NewErrorResponse(errors.Errorf("Unknown action %v", action))
		case actStatus:
			return s.Status(req)
		case actIdxGet:
			return s.Read(req)
		case actIdxSwap:
			return s.Swap(req)
		}
	}
}

func (s *server) Status(req net.Request) net.Response {
	return nil
	// roster, err := s.self.Roster(nil)
	// if err != nil {
		// return net.NewErrorResponse(err)
	// }
	// return statusRpc{s.self.peer.Id(), roster}.Response()
}

func (s *server) Read(req net.Request) net.Response {
	rpc, err := readGetRpc(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	item, ok, err := s.self.Read(nil, rpc)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return responseRpc{item, ok}.Response()
}

func (s *server) Swap(req net.Request) net.Response {
	rpc, err := readSwapRpc(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	item, ok, err := s.self.Swap(nil, rpc)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return responseRpc{item, ok}.Response()
}

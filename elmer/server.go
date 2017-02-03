package elmer

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
)

type server struct {
	ctx    common.Context
	logger common.Logger
	self   *machine
}

// Returns a new service handler for the ractlica
func newServer(ctx common.Context, listener net.Listener, self *machine) (net.Server, error) {
	return nil, nil
	// ctx = ctx.Sub("Server(%v)", listener.Addr())
//
	// server := &server{ctx: ctx, logger: ctx.Logger(), self: self}
	// return net.NewServer(ctx, ctx.Logger(), listener, serverInitHandler(server))
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
}

func (s *server) Read(req net.Request) net.Response {
	return nil
}

func (s *server) Swap(req net.Request) net.Response {
	return nil
}

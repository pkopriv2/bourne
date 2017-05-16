package warden

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/micro"
	"github.com/pkopriv2/bourne/net"
)

// Server endpoints
type rpcServer struct {
	ctx    common.Context
	logger common.Logger
	pub    PublicKey
	storage storage
}

// Returns a new service handler for the ractlica
func newServer(ctx common.Context, storage storage, listener net.Listener, workers int) (micro.Server, error) {
	server := &rpcServer{ctx: ctx, logger: ctx.Logger(), storage: storage}
	return micro.NewServer(ctx, listener, newServerHandler(server), workers)
}

func newServerHandler(s *rpcServer) func(micro.Request) micro.Response {
	return func(req micro.Request) micro.Response {
		if req.Body == nil {
			return micro.NewErrorResponse(errors.Errorf("Unknown request %v", req))
		}

		switch body := req.Body.(type) {
		default:
			panic("Unreachable")
		case rpcRegistration:
			return s.Register(body)
		}
	}
}

func (s *rpcServer) Register(r rpcRegistration) micro.Response {
	_, _, e := s.storage.SaveMember(r.Sub, r.Access)
	return micro.NewErrorResponse(e)
}

type rpcRegistration struct {
	// Token  Token
	Sub    Membership
	Access AccessShard
}

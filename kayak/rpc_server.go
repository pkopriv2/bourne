package kayak

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
)

type rpcServer struct {
	ctx    common.Context
	logger common.Logger
	self   *replica
}

// Returns a new service handler for the ractlica
func newServer(ctx common.Context, self *replica, listener net.Listener, workers int) (net.Server, error) {
	server := &rpcServer{ctx: ctx, logger: ctx.Logger(), self: self}
	return net.NewServer(ctx, listener, serverInitHandler(server), workers)
}

func serverInitHandler(s *rpcServer) func(net.Request) net.Response {
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
		case actReadBarrier:
			return s.ReadBarrier(req)
		case actReplicate:
			return s.Replicate(req)
		case actRequestVote:
			return s.RequestVote(req)
		case actAppend:
			return s.Append(req)
		case actUpdateRoster:
			return s.UpdateRoster(req)
		case actInstallSnapshot:
			return s.InstallSnapshot(req)
		}
	}
}

func (s *rpcServer) Status(req net.Request) net.Response {
	return status{s.self.Id, s.self.CurrentTerm(), s.self.Cluster()}.Response()
}

func (s *rpcServer) ReadBarrier(req net.Request) net.Response {
	val, err := s.self.ReadBarrier()
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return newReadBarrierResponse(val)
}

func (s *rpcServer) UpdateRoster(req net.Request) net.Response {
	update, err := readRosterUpdate(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return net.NewErrorResponse(s.self.UpdateRoster(update))
}

func (s *rpcServer) InstallSnapshot(req net.Request) net.Response {
	snapshot, err := readInstallSnapshot(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	resp, err := s.self.InstallSnapshot(snapshot)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return resp.Response()
}

func (s *rpcServer) Replicate(req net.Request) net.Response {
	replicate, err := readReplicate(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	resp, err := s.self.Replicate(replicate)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return resp.Response()
}

func (s *rpcServer) RequestVote(req net.Request) net.Response {
	voteRequest, err := readRequestVote(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	resp, err := s.self.RequestVote(voteRequest)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return resp.Response()
}

func (s *rpcServer) Append(req net.Request) net.Response {
	append, err := readAppendEvent(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	item, err := s.self.RemoteAppend(append)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return appendEventResponse{item.Index, item.Term}.Response()
}

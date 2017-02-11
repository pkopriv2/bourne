package convoy

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
)

type rpcServer struct {
	ctx     common.Context
	logger  common.Logger
	chs     *replica
	timeout time.Duration
}

// Returns a new service handler for the ractlica
func newServer(ctx common.Context, chs *replica, list net.Listener, workers int) (net.Server, error) {
	ctx = ctx.Sub("Server")

	server := &rpcServer{
		ctx:     ctx,
		logger:  ctx.Logger(),
		chs:     chs,
		timeout: 30 * time.Second,
	}

	return net.NewServer(ctx, list, serverInitHandler(server), workers)
}

func serverInitHandler(s *rpcServer) func(net.Request) net.Response {
	return func(req net.Request) net.Response {
		action, err := serverReadMeta(req.Meta())
		if err != nil {
			return net.NewErrorResponse(errors.Wrap(err, "Error parsing action"))
		}

		switch action {
		default:
			return net.NewErrorResponse(errors.Errorf("Unknown action %v", action))
		case actPing:
			return s.Ping(req)
		case actPingProxy:
			return s.ProxyPing(req)
		case actDirApply:
			return s.DirApply(req)
		case actDirList:
			return s.DirList(req)
		case actPushPull:
			return s.PushPull(req)
		}
	}
}

func (s *rpcServer) Ping(req net.Request) net.Response {
	return net.NewEmptyResponse()
}

// Handles a /dir/list request
func (s *rpcServer) ProxyPing(req net.Request) net.Response {
	id, err := readRpcPingProxyRequest(req)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	res, err := s.chs.ProxyPing(s.ctx.Timer(s.timeout), id)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return res.Response()
}

// Handles a /dir/list request
func (s *rpcServer) DirList(req net.Request) net.Response {
	res, err := s.chs.DirList(s.ctx.Timer(s.timeout))
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return res.Response()
}

// Handles a /dir/apply request
func (s *rpcServer) DirApply(req net.Request) net.Response {
	events, err := readRpcDirApplyRequest(req)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	res, err := s.chs.DirApply(s.ctx.Timer(s.timeout), events)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return res.Response()
}

// Handles a /evt/push request
func (s *rpcServer) PushPull(req net.Request) net.Response {
	rpc, err := readRpcPushPullRequest(req)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	res, err := s.chs.DirPushPull(s.ctx.Timer(s.timeout), rpc)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return res.Response()
}

package elmer

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
)

type rpcServer struct {
	ctx     common.Context
	logger  common.Logger
	self    *indexer
	roster  *roster
	timeout time.Duration
}

// Returns a new service handler for the ractlica
func newRpcServer(ctx common.Context, listener net.Listener, self *indexer, roster *roster, workers int, timeout time.Duration) (net.Server, error) {
	ctx = ctx.Sub("Rpc")

	server := &rpcServer{
		ctx:     ctx,
		logger:  ctx.Logger(),
		self:    self,
		roster:  roster,
		timeout: timeout}

	return net.NewServer(ctx, listener, rpcServerHandler(server), workers)
}

func rpcServerHandler(s *rpcServer) func(net.Request) net.Response {
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
		case actStoreInfo:
			return s.StoreInfo(req)
		case actStoreCreate:
			return s.StoreCreate(req)
		case actStoreDelete:
			return s.StoreDelete(req)
		case actStoreItemRead:
			return s.StoreItemRead(req)
		case actStoreItemSwap:
			return s.StoreItemSwap(req)
		}
	}
}

func (s *rpcServer) Status(req net.Request) net.Response {
	timer := s.ctx.Timer(s.timeout)
	defer timer.Close()

	roster, err := s.roster.Get(timer.Closed())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return statusRpc{s.self.peer.Id(), roster}.Response()
}

func (s *rpcServer) StoreInfo(req net.Request) net.Response {
	rpc, err := readPartialStoreRpc(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	timer := s.ctx.Timer(s.timeout)
	defer timer.Close()

	info, found, err := s.self.StoreInfo(timer.Closed(), rpc.Parent, rpc.Child)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return storeInfoRpc{info.Path, info.Enabled, found}.Response()
}

func (s *rpcServer) StoreCreate(req net.Request) net.Response {
	rpc, err := readStoreRequestRpc(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	timer := s.ctx.Timer(s.timeout)
	defer timer.Close()

	info, ok, err := s.self.StoreEnable(timer.Closed(), rpc.Store)
	if err != nil {
		return net.NewErrorResponse(errors.WithStack(err))
	}

	return storeInfoRpc{info.Path, info.Enabled, ok}.Response()
}

func (s *rpcServer) StoreDelete(req net.Request) net.Response {
	rpc, err := readStoreRequestRpc(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	timer := s.ctx.Timer(s.timeout)
	defer timer.Close()

	info, ok, err := s.self.StoreDisable(timer.Closed(), rpc.Store)
	if err != nil {
		return net.NewErrorResponse(err)
	}
	return storeInfoRpc{info.Path, info.Enabled, ok}.Response()
}

func (s *rpcServer) StoreItemRead(req net.Request) net.Response {
	rpc, err := readItemReadRpc(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	timer := s.ctx.Timer(s.timeout)
	defer timer.Close()

	item, ok, err := s.self.StoreItemRead(timer.Closed(), rpc.Store, rpc.Key)
	if err != nil {
		return net.NewErrorResponse(err)
	}
	return itemRpc{item, ok}.Response()
}

func (s *rpcServer) StoreItemSwap(req net.Request) net.Response {
	rpc, err := readSwapRpc(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	timer := s.ctx.Timer(s.timeout)
	defer timer.Close()

	item, ok, err := s.self.StoreItemSwap(timer.Closed(), rpc.Store, rpc.Key, rpc.Val, rpc.Ver, rpc.Del)
	if err != nil {
		return net.NewErrorResponse(err)
	}
	return itemRpc{item, ok}.Response()
}

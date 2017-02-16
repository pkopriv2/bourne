package elmer

import (
	"time"

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
		case actStoreExists:
			return s.StoreExists(req)
		case actStoreDel:
			return s.StoreDel(req)
		case actStoreEnsure:
			return s.StoreEnsure(req)
		case actStoreExistsItem:
			return s.StoreReaditem(req)
		case actStoreSwapItem:
			return s.StoreSwapItem(req)
		}
	}
}

func (s *server) Status(req net.Request) net.Response {
	// roster, err := s.self.Roster(nil)
	// if err != nil {
	// return net.NewErrorResponse(err)
	// }
	// return statusRpc{s.self.peer.Id(), roster}.Response()
	return nil
}

func (s *server) StoreExists(req net.Request) net.Response {
	rpc, err := readStoreRequestRpc(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	timer := s.ctx.Timer(30 * time.Second)
	defer timer.Close()

	ok, err := s.self.StoreExists(timer.Closed(), rpc.Store)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return storeResponseRpc{ok}.Response()
}

func (s *server) StoreDel(req net.Request) net.Response {
	rpc, err := readStoreRequestRpc(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	timer := s.ctx.Timer(30 * time.Second)
	defer timer.Close()

	if err := s.self.StoreDel(timer.Closed(), rpc.Store); err != nil {
		return net.NewErrorResponse(err)
	} else {
		return net.NewEmptyResponse()
	}
}

func (s *server) StoreEnsure(req net.Request) net.Response {
	rpc, err := readStoreRequestRpc(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	timer := s.ctx.Timer(30 * time.Second)
	defer timer.Close()

	if err := s.self.StoreEnsure(timer.Closed(), rpc.Store); err != nil {
		return net.NewErrorResponse(err)
	} else {
		return net.NewEmptyResponse()
	}
}

func (s *server) StoreReaditem(req net.Request) net.Response {
	rpc, err := readGetRpc(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	item, ok, err := s.self.StoreReadItem(nil, rpc.Store, rpc.Key)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return responseRpc{item, ok}.Response()
}

func (s *server) StoreSwapItem(req net.Request) net.Response {
	rpc, err := readSwapRpc(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	item, ok, err := s.self.StoreSwapItem(nil, rpc.Store, rpc.Key, rpc.Val, rpc.Ver)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return responseRpc{item, ok}.Response()
}

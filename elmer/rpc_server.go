package elmer

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
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

func (s *server) Status(req net.Request) net.Response {
	// roster, err := s.self.Roster(nil)
	// if err != nil {
	// return net.NewErrorResponse(err)
	// }
	// return statusRpc{s.self.peer.Id(), roster}.Response()
	return nil
}

func (s *server) StoreInfo(req net.Request) net.Response {
	rpc, err := readPartialStoreRpc(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	timer := s.ctx.Timer(30 * time.Second)
	defer timer.Close()

	ver, enabled, found, err := s.self.StoreInfo(timer.Closed(), rpc.Parent, rpc.Child)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return storeInfoRpc{path(rpc.Parent).Child(rpc.Child, ver), enabled, found}.Response()
}

func (s *server) StoreCreate(req net.Request) net.Response {
	rpc, err := readStoreRequestRpc(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	timer := s.ctx.Timer(30 * time.Second)
	defer timer.Close()

	ok, err := s.self.StoreEnableOrCreate(timer.Closed(), rpc.Store)
	if err != nil {
		return net.NewErrorResponse(err)
	}
	return net.NewStandardResponse(scribe.Write(scribe.BoolMessage(ok)))
}

func (s *server) StoreDelete(req net.Request) net.Response {
	rpc, err := readStoreRequestRpc(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	timer := s.ctx.Timer(30 * time.Second)
	defer timer.Close()

	ok, err := s.self.StoreDisable(timer.Closed(), rpc.Store)
	if err != nil {
		return net.NewErrorResponse(err)
	}
	return net.NewStandardResponse(scribe.Write(scribe.BoolMessage(ok)))
}

func (s *server) StoreItemRead(req net.Request) net.Response {
	rpc, err := readItemReadRpc(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	timer := s.ctx.Timer(30 * time.Second)
	defer timer.Close()

	item, ok, err := s.self.StoreItemRead(timer.Closed(), rpc.Store, rpc.Key)
	if err != nil {
		return net.NewErrorResponse(err)
	}
	return itemRpc{item, ok}.Response()
}

func (s *server) StoreItemSwap(req net.Request) net.Response {
	rpc, err := readSwapRpc(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	timer := s.ctx.Timer(30 * time.Second)
	defer timer.Close()

	item, ok, err := s.self.StoreItemSwap(timer.Closed(), rpc.Store, rpc.Swap)
	if err != nil {
		return net.NewErrorResponse(err)
	}
	return itemRpc{item, ok}.Response()
}

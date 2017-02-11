package elmer

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
)

type store struct {
	ctx    common.Context
	ctrl   common.Control
	logger common.Logger
	pool   common.ObjectPool // T: *rpcClient
}

func newStoreClient(ctx common.Context, network net.Network, addrs []string) *store {
	ctx = ctx.Sub("Store")
	return &store{
		ctx:    ctx,
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
		// pool:   common.NewObjectPool(ctx.Control(), 10, newClusterPool(ctx, network, addrs)),
	}
}

func (s *store) Close() error {
	return s.ctrl.Close()
}

func (s *store) Get(cancel <-chan struct{}, key []byte) (Item, bool, error) {
	raw := s.pool.TakeOrCancel(cancel)
	if raw == nil {
		return Item{}, false, errors.WithStack(CanceledError)
	}

	var err error
	defer func() {
		if err != nil {
			s.pool.Fail(raw)
		} else {
			s.pool.Return(raw)
		}
	}()
	resp, err := raw.(*rpcClient).Read(getRpc{key})
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	return resp.Item, resp.Ok, nil
}

func (s *store) Put(cancel <-chan struct{}, key []byte, val []byte, prev int) (Item, bool, error) {
	return s.Swap(cancel, key, val, prev)
}

func (s *store) Del(cancel <-chan struct{}, key []byte, prev int) (bool, error) {
	_, ok, err := s.Swap(cancel, key, nil, prev)
	return ok, err
}

func (s *store) Swap(cancel <-chan struct{}, key []byte, val []byte, prev int) (Item, bool, error) {
	raw := s.pool.TakeOrCancel(cancel)
	if raw == nil {
		return Item{}, false, errors.WithStack(CanceledError)
	}

	var err error
	defer func() {
		if err != nil {
			s.pool.Fail(raw)
		} else {
			s.pool.Return(raw)
		}
	}()

	resp, err := raw.(*rpcClient).Swap(swapRpc{key, val, prev})
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	return resp.Item, resp.Ok, nil
}

// type roster struct {
// }

// func newClusterPool(ctx common.Context, net net.Network, peers []string) func() (io.Closer, error) {
// perm := rand.Perm(len(peers))
// return func() (io.Closer, error) {
//
// }
// }
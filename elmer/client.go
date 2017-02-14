package elmer

import (
	"io"
	"math/rand"
	"time"

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

func newStoreClient(ctx common.Context, network net.Network, timeout time.Duration, refresh time.Duration, addrs []string) *store {
	ctx = ctx.Sub("Client")

	return &store{
		ctx:    ctx,
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
		pool:   common.NewObjectPool(ctx.Control(), 10, newClusterPool(ctx, roster)),
	}
}

func (s *store) Close() error {
	return s.ctrl.Close()
}

func (s *store) Get(cancel <-chan struct{}, key []byte) (Item, bool, error) {
	raw := s.pool.TakeOrCancel(cancel)
	if raw == nil {
		return Item{}, false, errors.WithStack(common.CanceledError)
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
		return Item{}, false, errors.WithStack(common.CanceledError)
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

func newClusterPool(ctx common.Context, m *rosterManager) func() (io.Closer, error) {
	return func() (io.Closer, error) {
		roster, err := m.Roster()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		cl, err := connect(ctx, m.net, m.timeout, roster[rand.Intn(len(roster))])
		if err != nil {
			return nil, err
		} else {
			return cl, nil
		}
	}
}

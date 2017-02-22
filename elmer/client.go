package elmer

import (
	"io"
	"math/rand"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
)

func tryRpc(pool common.ObjectPool, cancel <-chan struct{}, fn func(*rpcClient) error) error {
	raw := pool.TakeOrCancel(cancel)
	if raw == nil {
		return errors.WithStack(common.CanceledError)
	}
	var err error
	defer func() {
		if err != nil {
			pool.Fail(raw)
		} else {
			pool.Return(raw)
		}
	}()

	err = fn(raw.(*rpcClient))
	return err
}

type peerClient struct {
	ctx    common.Context
	ctrl   common.Control
	logger common.Logger
	pool   common.ObjectPool // T: *rpcClient
}

func newPeerClient(ctx common.Context, net net.Network, timeout time.Duration, max int, addrs []string) *peerClient {
	ctx = ctx.Sub("Client")

	pool := common.NewObjectPool(ctx.Control(), max,
		newStaticClusterPool(ctx, net, timeout, addrs))
	ctx.Control().Defer(func(error) {
		pool.Close()
	})

	return &peerClient{
		ctx:    ctx,
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
		pool:   pool,
	}
}

func (p *peerClient) Close() error {
	return p.ctrl.Close()
}

func (p *peerClient) Shutdown() error {
	panic("Not implemented")
}

type storeClient struct {
	path   []segment
	logger common.Logger
	ctrl   common.Control
	pool   common.ObjectPool // T: *rpcClient
}

func newStoreClient(ctx common.Context, pool common.ObjectPool, path []segment) *storeClient {
	ctx = ctx.Sub("StoreClient(%v)", path)
	return &storeClient{path, ctx.Logger(), ctx.Control(), pool}
}

func (s *storeClient) Close() error {
	return s.ctrl.Close()
}

func (s *storeClient) Name() []byte {
	return path(s.path).Last().Elem
}

func (s *storeClient) GetStore(cancel <-chan struct{}, name []byte) (Store, error) {
	// err := tryRpc(s.pool, cancel, func(r *rpcClient) error {
	// responseRpc, err := r.Store(getRpc{s.path, key})
	// if err != nil {
	// return err
	// }
	//
	// item, ok = responseRpc.Item, responseRpc.Ok
	// return nil
	// })
	// return nil, err
	panic("not implemented")
}

func (s *storeClient) CreateStore(cancel <-chan struct{}, name []byte) (Store, error) {
	panic("not implemented")
}

func (s *storeClient) DeleteStore(cancel <-chan struct{}, name []byte) error {
	panic("not implemented")
}

func (s *storeClient) Get(cancel <-chan struct{}, key []byte) (Item, bool, error) {
	if s.ctrl.IsClosed() {
		return Item{}, false, errors.WithStack(common.ClosedError)
	}
	return s.Read(cancel, key)
}

func (s *storeClient) Del(cancel <-chan struct{}, key []byte, prev int) (bool, error) {
	if s.ctrl.IsClosed() {
		return false, errors.WithStack(common.ClosedError)
	}
	_, o, e := s.Swap(cancel, key, []byte{}, prev, true)
	return o, e
}

func (s *storeClient) Put(cancel <-chan struct{}, key []byte, val []byte, prev int) (Item, bool, error) {
	if s.ctrl.IsClosed() {
		return Item{}, false, errors.WithStack(common.ClosedError)
	}
	return s.Swap(cancel, key, val, prev, false)
}

func (s *storeClient) Read(cancel <-chan struct{}, key []byte) (Item, bool, error) {
	defer common.Elapsed(s.logger, "Get", time.Now())

	item, ok := Item{}, false
	err := tryRpc(s.pool, cancel, func(r *rpcClient) error {
		responseRpc, err := r.StoreItemRead(itemReadRpc{s.path, key})
		if err != nil {
			return err
		}

		item, ok = responseRpc.Item, responseRpc.Ok
		return nil
	})
	return item, ok, err
}

func (s *storeClient) Swap(cancel <-chan struct{}, key []byte, val []byte, prev int, del bool) (Item, bool, error) {
	if s.ctrl.IsClosed() {
		return Item{}, false, errors.WithStack(common.ClosedError)
	}
	defer common.Elapsed(s.logger, "Swap", time.Now())

	item, ok := Item{}, false
	err := tryRpc(s.pool, cancel, func(r *rpcClient) error {
		responseRpc, err := r.StoreItemSwap(swapRpc{s.path, Item{key, val, prev, del}})
		if err != nil {
			return err
		}
		item, ok = responseRpc.Item, responseRpc.Ok
		return nil
	})
	return item, ok, err
}

func newStaticClusterPool(ctx common.Context, net net.Network, timeout time.Duration, addrs []string) func() (io.Closer, error) {
	return func() (io.Closer, error) {
		cl, err := connect(ctx, net, timeout, addrs[rand.Intn(len(addrs))])
		if err != nil {
			return nil, err
		} else {
			return cl, nil
		}
	}
}

func newDynamicClusterPool(ctx common.Context, m *rosterSync) func() (io.Closer, error) {
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

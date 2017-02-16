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

func (p *peerClient) Catalog() (Catalog, error) {
	ctx := p.ctx.Sub("Catalog")
	return &catalogClient{ctx, ctx.Control(), ctx.Logger(), p.pool}, nil
}

func (p *peerClient) Shutdown() error {
	panic("Not implemented")
}

type catalogClient struct {
	ctx    common.Context
	ctrl   common.Control
	logger common.Logger
	pool   common.ObjectPool // T: *rpcClient
}

func newCatalogClient(ctx common.Context, pool common.ObjectPool) *catalogClient {
	ctx = ctx.Sub("Catalog")
	return &catalogClient{ctx, ctx.Control(), ctx.Logger(), pool}
}

func (c *catalogClient) Close() error {
	return c.ctrl.Close()
}

func (c *catalogClient) Del(cancel <-chan struct{}, store []byte) error {
	if c.ctrl.IsClosed() {
		return errors.WithStack(common.ClosedError)
	}

	return tryRpc(c.pool, cancel, func(r *rpcClient) error {
		return r.StoreDel(storeRequestRpc{store})
	})
}

func (c *catalogClient) Get(cancel <-chan struct{}, store []byte) (Store, error) {
	if c.ctrl.IsClosed() {
		return nil, errors.WithStack(common.ClosedError)
	}

	var ok bool
	err := tryRpc(c.pool, cancel, func(r *rpcClient) error {
		resp, err := r.StoreExists(storeRequestRpc{store})
		if err != nil {
			return err
		}

		ok = resp.Ok
		return nil
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !ok {
		return nil, nil
	}

	return newStoreClient(c.ctx, c.pool, store), nil
}

func (c *catalogClient) Ensure(cancel <-chan struct{}, store []byte) (Store, error) {
	if c.ctrl.IsClosed() {
		return nil, errors.WithStack(common.ClosedError)
	}

	err := tryRpc(c.pool, cancel, func(r *rpcClient) error {
		return r.StoreEnsure(storeRequestRpc{store})
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return newStoreClient(c.ctx, c.pool, store), nil
}

type storeClient struct {
	name   []byte
	logger common.Logger
	ctrl   common.Control
	pool   common.ObjectPool // T: *rpcClient
}

func newStoreClient(ctx common.Context, pool common.ObjectPool, name []byte) *storeClient {
	ctx = ctx.Sub("StoreClient: %v", string(name))
	return &storeClient{name, ctx.Logger(), ctx.Control(), pool}
}

func (s *storeClient) Close() error {
	return s.ctrl.Close()
}

func (s *storeClient) Name() []byte {
	return s.name
}

func (s *storeClient) Get(cancel <-chan struct{}, key []byte) (Item, bool, error) {
	if s.ctrl.IsClosed() {
		return Item{}, false, errors.WithStack(common.ClosedError)
	}
	defer common.Elapsed(s.logger, "Get", time.Now())

	var item Item
	var ok bool
	err := tryRpc(s.pool, cancel, func(r *rpcClient) error {
		responseRpc, err := r.StoreGetItem(getRpc{s.name, key})
		if err != nil {
			return err
		}

		item, ok = responseRpc.Item, responseRpc.Ok
		return nil
	})
	return item, ok, err
}

func (s *storeClient) Put(cancel <-chan struct{}, key []byte, val []byte, ver int) (Item, bool, error) {
	if s.ctrl.IsClosed() {
		return Item{}, false, errors.WithStack(common.ClosedError)
	}
	defer common.Elapsed(s.logger, "Put", time.Now())

	var item Item
	var ok bool

	err := tryRpc(s.pool, cancel, func(r *rpcClient) error {
		responseRpc, err := r.StoreSwapItem(swapRpc{s.name, key, val, ver})
		if err != nil {
			return err
		}
		item, ok = responseRpc.Item, responseRpc.Ok
		return nil
	})
	return item, ok, err
}

func (s *storeClient) Del(cancel <-chan struct{}, key []byte, ver int) (bool, error) {
	if s.ctrl.IsClosed() {
		return false, errors.WithStack(common.ClosedError)
	}
	defer common.Elapsed(s.logger, "Del", time.Now())

	var ok bool
	err := tryRpc(s.pool, cancel, func(r *rpcClient) error {
		responseRpc, err := r.StoreSwapItem(swapRpc{s.name, key, nil, ver})
		if err != nil {
			return err
		}
		ok = responseRpc.Ok
		return nil
	})
	return ok, err
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

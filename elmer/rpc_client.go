package elmer

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
)

type rpcClient struct {
	raw net.Client
}

func connect(ctx common.Context, network net.Network, timeout time.Duration, addr string) (*rpcClient, error) {
	conn, err := network.Dial(timeout, addr)
	if conn == nil || err != nil {
		return nil, errors.Wrapf(err, "Unable to connect to [%v]", addr)
	}
	defer func() {
		if err != nil {
			conn.Close()
		}
	}()

	cl, err := net.NewClient(ctx, conn, net.Json)
	if cl == nil || err != nil {
		return nil, errors.Wrapf(err, "Unable to connect to [%v]", addr)
	}

	return newClient(cl), nil
}

func newClient(raw net.Client) *rpcClient {
	return &rpcClient{raw}
}

func (c *rpcClient) Close() error {
	return c.raw.Close()
}

func (c *rpcClient) Status() (statusRpc, error) {
	resp, err := c.raw.Send(newStatusRequest())
	if err != nil {
		return statusRpc{}, err
	}

	if err := resp.Error(); err != nil {
		return statusRpc{}, err
	}

	return readStatusRpc(resp.Body())
}

func (c *rpcClient) StoreExists(s storeRequestRpc) (storeResponseRpc, error) {
	resp, err := c.raw.Send(s.Exists())
	if err != nil {
		return storeResponseRpc{}, errors.WithStack(err)
	}

	if err := resp.Error(); err != nil {
		return storeResponseRpc{}, errors.WithStack(err)
	}

	return readStoreResponseRpc(resp.Body())
}

func (c *rpcClient) StoreDel(s storeRequestRpc) error {
	resp, err := c.raw.Send(s.Del())
	if err != nil {
		return errors.WithStack(err)
	}

	if err := resp.Error(); err != nil {
		return errors.WithStack(err)
	} else {
		return nil
	}
}

func (c *rpcClient) StoreEnsure(s storeRequestRpc) error {
	resp, err := c.raw.Send(s.Ensure())
	if err != nil {
		return errors.WithStack(err)
	}

	if err := resp.Error(); err != nil {
		return errors.WithStack(err)
	} else {
		return nil
	}
}

func (c *rpcClient) StoreGetItem(g getRpc) (responseRpc, error) {
	resp, err := c.raw.Send(g.Request())
	if err != nil {
		return responseRpc{}, errors.WithStack(err)
	}

	if err := resp.Error(); err != nil {
		return responseRpc{}, errors.WithStack(err)
	}

	return readResponseRpc(resp.Body())
}

func (c *rpcClient) StoreSwapItem(s swapRpc) (responseRpc, error) {
	resp, err := c.raw.Send(s.Request())
	if err != nil {
		return responseRpc{}, errors.WithStack(err)
	}

	if err := resp.Error(); err != nil {
		return responseRpc{}, errors.WithStack(err)
	}

	return readResponseRpc(resp.Body())
}

// type rpcClientPool struct {
// ctx common.Context
// raw common.ObjectPool
// }
//
// func newRpcClientPool(ctx common.Context, network net.Network, peer peer, size int) *rpcClientPool {
// ctx = ctx.Sub("ClientPool(%v,%v)", peer, size)
// return &rpcClientPool{ctx, common.NewObjectPool(ctx, size, newRpcClientConstructor(ctx, network, peer))}
// }
//
// func (c *rpcClientPool) Close() error {
// return c.raw.Close()
// }
//
// func (c *rpcClientPool) Max() int {
// return c.raw.Max()
// }
//
// func (c *rpcClientPool) TakeTimeout(dur time.Duration) *rpcClient {
// raw := c.raw.TakeTimeout(dur)
// if raw == nil {
// return nil
// }
//
// return raw.(*rpcClient)
// }
//
// func (c *rpcClientPool) Return(cl *rpcClient) {
// c.raw.Return(cl)
// }
//
// func (c *rpcClientPool) Fail(cl *rpcClient) {
// c.raw.Fail(cl)
// }
//
// func newRpcClientConstructor(ctx common.Context, network net.Network, addrs []string) func() (io.Closer, error) {
// return func() (io.Closer, error) {
// if cl, err := peer.Client(ctx, network, 30*time.Second); cl != nil && err == nil {
// return cl, err
// }
//
// return nil, nil
// }
// }

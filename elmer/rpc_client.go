package elmer

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
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

func (c *rpcClient) StoreInfo(s partialStoreRpc) (storeInfoRpc, error) {
	resp, err := c.raw.Send(s.Request())
	if err != nil {
		return storeInfoRpc{}, errors.WithStack(err)
	}

	if err := resp.Error(); err != nil {
		return storeInfoRpc{}, errors.WithStack(err)
	}

	return readStoreInfoRpc(resp.Body())
}

func (c *rpcClient) StoreDelete(s storeRpc) (bool, error) {
	resp, err := c.raw.Send(s.Delete())
	if err != nil {
		return false, errors.WithStack(err)
	}

	if err := resp.Error(); err != nil {
		return false, errors.WithStack(err)
	}

	ok, err := scribe.ReadBoolMessage(resp.Body())
	if err != nil {
		return false, errors.WithStack(err)
	}
	return ok, nil
}

func (c *rpcClient) StoreCreate(s storeRpc) (bool, error) {
	resp, err := c.raw.Send(s.Create())
	if err != nil {
		return false, errors.WithStack(err)
	}

	if err := resp.Error(); err != nil {
		return false, errors.WithStack(err)
	}

	ok, err := scribe.ReadBoolMessage(resp.Body())
	if err != nil {
		return false, errors.WithStack(err)
	}
	return ok, nil
}

func (c *rpcClient) StoreItemRead(g itemReadRpc) (itemRpc, error) {
	resp, err := c.raw.Send(g.Request())
	if err != nil {
		return itemRpc{}, errors.WithStack(err)
	}

	if err := resp.Error(); err != nil {
		return itemRpc{}, errors.WithStack(err)
	}

	return readItemRpc(resp.Body())
}

func (c *rpcClient) StoreItemSwap(s swapRpc) (itemRpc, error) {
	resp, err := c.raw.Send(s.Request())
	if err != nil {
		return itemRpc{}, errors.WithStack(err)
	}

	if err := resp.Error(); err != nil {
		return itemRpc{}, errors.WithStack(err)
	}

	return readItemRpc(resp.Body())
}

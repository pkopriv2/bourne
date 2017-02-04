package kayak

import (
	"io"
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

func (c *rpcClient) Barrier() (int, error) {
	resp, err := c.raw.Send(newReadBarrierRequest())
	if err != nil {
		return 0, err
	}

	if err := resp.Error(); err != nil {
		return 0, err
	}

	return readBarrierResponse(resp.Body())
}

func (c *rpcClient) Status() (status, error) {
	resp, err := c.raw.Send(newStatusRequest())
	if err != nil {
		return status{}, err
	}

	if err := resp.Error(); err != nil {
		return status{}, err
	}

	return readStatusResponse(resp.Body())
}

func (c *rpcClient) UpdateRoster(peer peer, join bool) error {
	resp, err := c.raw.Send(rosterUpdate{peer, join}.Request())
	if err != nil {
		return err
	}

	if err := resp.Error(); err != nil {
		return err
	} else {
		return nil
	}
}

func (c *rpcClient) Replicate(r replicate) (response, error) {
	resp, err := c.raw.Send(r.Request())
	if err != nil {
		return response{}, errors.Wrapf(err, "Error sending replicate events: %v", r)
	}

	if err := resp.Error(); err != nil {
		return response{}, err
	}

	return readResponse(resp.Body())
}

func (c *rpcClient) Append(a appendEvent) (appendEventResponse, error) {
	resp, err := c.raw.Send(a.Request())
	if err != nil {
		return appendEventResponse{}, err
	}

	if err := resp.Error(); err != nil {
		return appendEventResponse{}, err
	}

	return readAppendEventResponse(resp.Body())
}

func (c *rpcClient) InstallSnapshot(snapshot installSnapshot) (response, error) {
	resp, err := c.raw.Send(snapshot.Request())
	if err != nil {
		return response{}, errors.Wrapf(err, "Error sending snapshot: %v", snapshot)
	}

	if err := resp.Error(); err != nil {
		return response{}, err
	}

	return readResponse(resp.Body())
}

func (c *rpcClient) RequestVote(vote requestVote) (response, error) {
	resp, err := c.raw.Send(vote.Request())
	if err != nil {
		return response{}, errors.Wrapf(err, "Error sending request vote: %v", vote)
	}

	if err := resp.Error(); err != nil {
		return response{}, err
	}

	return readResponse(resp.Body())
}

type rpcClientPool struct {
	ctx common.Context
	raw common.ObjectPool
}

func newRpcClientPool(ctx common.Context, network net.Network, peer peer, size int) *rpcClientPool {
	return &rpcClientPool{ctx, common.NewObjectPool(ctx.Control(), size, newRpcClientConstructor(ctx, network, peer))}
}

func (c *rpcClientPool) Close() error {
	return c.raw.Close()
}

func (c *rpcClientPool) Max() int {
	return c.raw.Max()
}

func (c *rpcClientPool) TakeTimeout(dur time.Duration) *rpcClient {
	raw := c.raw.TakeTimeout(dur)
	if raw == nil {
		return nil
	}

	return raw.(*rpcClient)
}

func (c *rpcClientPool) Return(cl *rpcClient) {
	c.raw.Return(cl)
}

func (c *rpcClientPool) Fail(cl *rpcClient) {
	c.raw.Fail(cl)
}

func newRpcClientConstructor(ctx common.Context, network net.Network, peer peer) func() (io.Closer, error) {
	return func() (io.Closer, error) {
		if cl, err := peer.Client(ctx, network, 30*time.Second); cl != nil && err == nil {
			return cl, err
		}

		return nil, nil
	}
}

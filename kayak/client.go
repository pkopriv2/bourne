package kayak

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

type Client struct {
	peers []string
}

type rpcClient struct {
	raw net.Client
}

func connect(ctx common.Context, addr string) (*rpcClient, error) {
	cl, err := net.NewTcpClient(ctx, ctx.Logger(), addr)
	if err != nil {
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

func (c *rpcClient) Append(e Event, source uuid.UUID, seq int, kind Kind) (LogItem, error) {
	return LogItem{}, nil
	// resp, err := c.raw.Send(appendEventRequest{e, source, seq, kind}.Request())
	// if err != nil {
	// return LogItem{}, err
	// }
	//
	// res, err := readNetAppendEventResponse(resp)
	// if err != nil {
	// return LogItem{}, errors.Wrapf(err, "Error receiving append response: %v", e)
	// }
	//
	// return LogItem{res.index, e, res.term, source, seq, kind}, nil
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
	raw net.ClientPool
}

func newRpcClientPool(ctx common.Context, raw net.ClientPool) *rpcClientPool {
	return &rpcClientPool{ctx, raw}
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

	return &rpcClient{raw}
}

func (c *rpcClientPool) Return(cl *rpcClient) {
	c.raw.Return(cl.raw)
}

func (c *rpcClientPool) Fail(cl *rpcClient) {
	c.raw.Fail(cl.raw)
}

package convoy

import (
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// A thin member client.  This class should remain 1:1 with the
// server handler.  The convention currently is that server.go
// host the shared request/response types.
type rpcClient struct {
	Raw net.Client
}

// Connects to the given member at addr and returns the standard member client.
func connectMember(ctx common.Context, network net.Network, timeout time.Duration, addr string) (*rpcClient, error) {
	ctx = ctx.Sub("Client(%v)", addr)

	conn, err := network.Dial(timeout, addr)
	if err != nil {
		return nil, err
	}

	raw, err := net.NewClient(ctx, conn, net.Json)
	if err != nil {
		return nil, err
	}

	return &rpcClient{raw}, nil
}

func (c *rpcClient) Close() error {
	return c.Raw.Close()
}

func (m *rpcClient) Ping() error {
	_, err := m.Raw.Send(newPingRequest())
	return err
}

func (m *rpcClient) PingProxy(target uuid.UUID) (bool, error) {
	resp, err := m.Raw.Send(rpcPingProxyRequest(target).Request())
	if err != nil {
		return false, err
	}

	if err := resp.Error(); err != nil {
		return false, err
	}

	return readRpcPingResponse(resp)
}

func (m *rpcClient) DirList() ([]event, error) {
	resp, err := m.Raw.Send(newDirListRequest())
	if err != nil {
		return nil, err
	}

	if err := resp.Error(); err != nil {
		return nil, err
	}

	return readRpcDirListResponse(resp)
}

func (m *rpcClient) DirApply(events []event) ([]bool, error) {
	resp, err := m.Raw.Send(rpcDirApplyRequest(events).Request())
	if err != nil {
		return nil, err
	}

	if err := resp.Error(); err != nil {
		return nil, err
	}

	return readRpcDirApplyResponse(resp)
}

func (m *rpcClient) DissemPushPull(sourceId uuid.UUID, sourceVersion int, events []event) ([]bool, []event, error) {
	resp, err := m.Raw.Send(rpcPushPullRequest{sourceId, sourceVersion, events}.Request())
	if err != nil {
		return nil, nil, err
	}

	if err := resp.Error(); err != nil {
		return nil, nil, err
	}

	rpc, err := readRpcPushPullResponse(resp)
	if err != nil {
		return nil, nil, err
	}

	return rpc.success, rpc.events, nil
}

func (m *rpcClient) StorePut(key string, val string, expected int) ([]bool, []event, error) {
	return nil, nil, nil
	// resp, err := m.Raw.Send(newPushPullRequest(sourceId, sourceVersion, events))
	// if err != nil {
	// return nil, nil, err
	// }
	//
	// success, events, err := readPushPullResponse(resp)
	// if err != nil {
	// switch err.Error() {
	// default:
	// return nil, nil, err
	// case EvictedError.Error():
	// return nil, nil, EvictedError
	// case FailedError.Error():
	// return nil, nil, FailedError
	// }
	// }
	//
	// return success, events, nil
}

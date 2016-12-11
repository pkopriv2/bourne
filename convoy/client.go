package convoy

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// A thin member client.
type client struct {
	Raw net.Client
}

// Connects to the given member at addr and returns the standard member client.
func connectMember(ctx common.Context, log common.Logger, addr string) (*client, error) {
	conn, err := net.ConnectTcp(addr)
	if err != nil {
		return nil, err
	}

	raw, err := net.NewClient(ctx, log.Fmt("-> [%v]", addr), conn)
	if err != nil {
		return nil, err
	}

	return &client{raw}, nil
}

func (c *client) Close() error {
	return c.Raw.Close()
}

func (m *client) Ping() error {
	_, err := m.Raw.Send(newPingRequest())
	if err != nil {
		return err
	}
	return nil
}

func (m *client) PingProxy(target uuid.UUID) (bool, error) {
	resp, err := m.Raw.Send(newPingProxyRequest(target))
	if err != nil {
		return false, err
	}

	return readPingProxyResponse(resp)
}

func (m *client) DirList() ([]event, error) {
	resp, err := m.Raw.Send(newDirListRequest())
	if err != nil {
		return nil, err
	}

	return readDirListResponse(resp)
}

func (m *client) DirApply(events []event) ([]bool, error) {
	resp, err := m.Raw.Send(newDirApplyRequest(events))
	if err != nil {
		return nil, err
	}

	return readDirApplyResponse(resp)
}

func (m *client) PushPull(source uuid.UUID, events []event) ([]bool, []event, error) {
	resp, err := m.Raw.Send(newPushPullRequest(source, events))
	if err != nil {
		return nil, nil, err
	}

	success, events, err := readPushPullResponse(resp)
	if err != nil {
		switch err.Error() {
		default:
			return nil, nil, err
		case replicaEvictedError.Error():
			return nil, nil, replicaEvictedError
		case replicaFailureError.Error():
			return nil, nil, replicaFailureError
		}
	}

	return success, events, nil
}

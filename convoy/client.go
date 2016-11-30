package convoy

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
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

func (m *client) EvtPushPull(events []event) ([]bool, []event, error) {
	resp, err := m.Raw.Send(newEvtPushPullRequest(events))
	if err != nil {
		return nil, nil, err
	}

	return readEvtPushPullResponse(resp)
}

package convoy

import (
	"fmt"
	"strconv"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

type member struct {
	Id      uuid.UUID
	Host    string
	Port    string // no need for int representation
	Version int
}

func newMember(id uuid.UUID, host string, port string, ver int) *member {
	return &member{id, host, port, ver}
}

func (m *member) String() string {
	return fmt.Sprintf("(%v)[%v:%v]", m.Id.String(), m.Host, m.Port)
}

func (m *member) connect(port string) (net.Connection, error) {
	return net.ConnectTcp(net.NewAddr(m.Host, port))
}

func (m *member) Connect(port int) (net.Connection, error) {
	return m.connect(strconv.Itoa(port))
}

func (m *member) Store(common.Context) (Store, error) {
	panic("not implemented")
}

func (m *member) Client(ctx common.Context) (*client, error) {
	conn, err := m.connect(m.Port)
	if err != nil {
		return nil, err
	}

	raw, err := net.NewClient(ctx, conn)
	if err != nil {
		return nil, err
	}

	return &client{raw}, nil
}

// A thin member client.
type client struct {
	Raw net.Client
}

func (c *client) Close() error {
	return c.Raw.Close()
}

func (m *client) DirList(ctx common.Context, events []event) ([]event, error) {
	resp, err := m.Raw.Send(newDirListRequest())
	if err != nil {
		return nil, err
	}

	return readDirListResponse(resp)
}

func (m *client) DirApply(ctx common.Context, events []event) ([]bool, error) {
	resp, err := m.Raw.Send(newDirApplyRequest(events))
	if err != nil {
		return nil, err
	}

	return readDirApplyResponse(resp)
}

package convoy

import (
	"fmt"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

type member struct {
	id   uuid.UUID
	host string
	port int
}

func newMember(id uuid.UUID, host string, port int) *member {
	return &member{id, host, port}
}

func (m *member) String() string {
	return fmt.Sprintf("(%v)[%v:%v]", m.id.String(), m.host, m.port)
}

func (m *member) Close() error {
	panic("not implemented")
}

func (m *member) Ping() bool {
	panic("not implemented")
}

func (m *member) Update(Status int) error {
	panic("not implemented")
}

func (m *member) Connect(port int) (net.Connection, error) {
	return net.ConnectTcp(net.NewAddr(m.host, port))
}

func (m *member) Store(common.Context) (Store, error) {
	panic("not implemented")
}

package convoy

import (
	"fmt"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/enc"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

type member struct {
	Id   uuid.UUID
	Host string
	Port string // no need for int representation
	Version int
}

func (m *member) Write(enc.Writer) {
	panic("not implemented")
}

func newMember(id uuid.UUID, host string, port string, ver int) *member {
	return &member{id, host, port, ver}
}

func (m *member) String() string {
	return fmt.Sprintf("(%v)[%v:%v]", m.Id.String(), m.Host, m.Port)
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
	return net.ConnectTcp(net.NewAddr(m.Host, m.Port))
}

func (m *member) Store(common.Context) (Store, error) {
	panic("not implemented")
}

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
	return fmt.Sprintf("Member(%v)[%v:%v]", m.Id.String()[:7], m.Host, m.Port)
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
	return connectMember(ctx, ctx.Logger().Fmt(m.String()), net.NewAddr(m.Host, m.Port))
}

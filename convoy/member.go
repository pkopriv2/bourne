package convoy

import (
	"fmt"
	"strconv"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

type MemberStatus int

func (s MemberStatus) String() string {
	switch s {
	default:
		return "Unknown"
	case Alive:
		return "Alive"
	case Failed:
		return "Failed"
	}
}

// member statuses
const (
	Alive   MemberStatus = 0
	Failed               = 1
	Unknown              = 2
)

// member implementation.  For now, these should be considered immutable.
type member struct {
	Id      uuid.UUID
	Host    string
	Port    string // no need for int representation
	Version int
	Status  MemberStatus
}

func newMember(id uuid.UUID, host string, port string, ver int, status MemberStatus) *member {
	return &member{id, host, port, ver, status}
}

func (m *member) String() string {
	return fmt.Sprintf("Member(id=%v)[addr=%v:%v](status=%v,ver=%v)", m.Id.String()[:7], m.Host, m.Port, m.Status, m.Version)
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

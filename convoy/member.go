package convoy

import (
	"fmt"
	"strconv"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

func membersCollect(arr []member, fn func(m member) bool) []member {
	ret := make([]member, 0, len(arr))
	for _, m := range arr {
		if fn(m) {
			ret = append(ret, m)
		}
	}
	return ret
}

// member implementation.  For now, these should be considered immutable.
type member struct {
	Id      uuid.UUID
	Host    string
	Port    string // no need for int representation
	Version int
	Healthy bool
	Active  bool
}

func newMember(id uuid.UUID, host string, port string, ver int) member {
	return member{id, host, port, ver, true, true}
}

func (m member) String() string {
	return fmt.Sprintf("Member(id=%v)[addr=%v:%v](v=%v)", m.Id.String()[:8], m.Host, m.Port, m.Version)
}

func (m member) connect(port string) (net.Connection, error) {
	return net.ConnectTcp(net.NewAddr(m.Host, port))
}

func (m member) Connect(port int) (net.Connection, error) {
	return m.connect(strconv.Itoa(port))
}

func (m member) Store(common.Context) (Store, error) {
	panic("not implemented")
}

func (m member) Client(ctx common.Context) (*client, error) {
	return connectMember(ctx, ctx.Logger().Fmt(m.String()), net.NewAddr(m.Host, m.Port))
}

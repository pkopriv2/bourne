package convoy

import (
	"fmt"
	"strconv"
	"time"

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

func membersAddrs(all []member) []string {
	ret := make([]string, 0, len(all))
	for _, m := range all {
		ret = append(ret, m.Addr())
	}
	return ret
}

// member implementation.  For now, these should be considered immutable.
type member struct {
	id      uuid.UUID
	host    string
	port    string // no need for int representation
	version int
	healthy bool
	active  bool
}

func newMember(id uuid.UUID, host string, port string, ver int) member {
	return member{id, host, port, ver, true, true}
}

func (m member) Id() uuid.UUID {
	return m.id
}

func (m member) Addr() string {
	return net.NewAddr(m.host, m.port)
}

func (m member) Hostname() string {
	return m.host
}

func (m member) Version() int {
	return m.version
}

func (m member) String() string {
	return fmt.Sprintf("Member(%v,%v,%v)", m.id.String()[:8], m.version, net.NewAddr(m.host, m.port))
}

func (m member) Connect(network net.Network, timeout time.Duration, port int) (net.Connection, error) {
	return network.Dial(timeout, net.NewAddr(m.host, strconv.Itoa(port)))
}

func (m member) Client(ctx common.Context, network net.Network, timeout time.Duration) (*rpcClient, error) {
	return connectMember(ctx, network, timeout, net.NewAddr(m.host, m.port))
}

func (m member) Store(net net.Network, timeout time.Duration) (Store, error) {
	panic("not implemented")
}

func (m member) Directory(net net.Network, timeout time.Duration) (Directory, error) {
	panic("not implemented")
}

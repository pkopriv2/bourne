package convoy

import (
	"sync"

	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// func ParseMemberFromJson(bytes []byte) (Member, error) {
// m := MemberData{}
// // if err := json.Unmarshal(bytes, data); err != nil {
// // return nil, err
// // }
// //
// // return newMember(m.Id, m.Factory)
// }

type MemberData struct {
	Id      string
	Version int
	Factory []byte
}

// TODO: members must be gob encodable!  connection factory can't be encoded!
type memberImpl struct {
	id  uuid.UUID
	fac net.ConnectionFactory
	ver int
}

func newMember(id uuid.UUID, fac net.ConnectionFactory, ver int) Member {
	return &memberImpl{id, fac, ver}
}

func (m *memberImpl) Id() uuid.UUID {
	return m.id
}

func (m *memberImpl) Conn() (net.Connection, error) {
	return m.fac.Conn()
}

func (m *memberImpl) Version() int {
	return m.ver
}

func (m *memberImpl) serialize() interface{} {
	return nil
}

func (m *memberImpl) client() (client, error) {
	conn, err := m.Conn()
	if err != nil {
		return nil, err
	}

	return newClient(net.NewClient(conn)), nil
}

type clientImpl struct {
	client net.Client
	lock   sync.Mutex
}

func newClient(client net.Client) client {
	return &clientImpl{client: client}
}

func (c *clientImpl) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.client.Close()
}

func (c *clientImpl) Ping() (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	_, err := c.client.Send(newPingRequest())
	if err != nil {
		return false, err
	}

	return true, nil
}

func (c *clientImpl) PingProxy(id uuid.UUID) (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	resp, err := c.client.Send(newPingProxyRequest(id))
	if err != nil {
		return false, err
	}

	return parsePingProxyResponse(resp)
}

func (c *clientImpl) Update(updates []update) ([]bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	resp, err := c.client.Send(newUpdateRequest(updates))
	if err != nil {
		return nil, err
	}

	return parseUpdateResponse(resp)
}

package convoy

import (
	"github.com/go-errors/errors"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/enc"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

func readMember(ctx common.Context, r enc.Reader) (Member, error) {
	var id string
	var version int
	var connection enc.Message
	if err := r.Read("id", &id); err != nil {
		return nil, errors.New(err)
	}
	if err := r.Read("version", &version); err != nil {
		return nil, errors.New(err)
	}
	if err := r.Read("connection", &connection); err != nil {
		return nil, errors.New(err)
	}

	factory, err := net.ReadConnectionFactory(connection)
	if err != nil {
		return nil, errors.New(err)
	}

	uid, err := uuid.FromString(id)
	if err != nil {
		return nil, errors.New(err)
	}

	return newMember(ctx, uid, factory, version), nil
}

type member struct {
	ctx     common.Context
	id      uuid.UUID
	factory net.ConnectionFactory
	version int
}

func newMember(ctx common.Context, id uuid.UUID, factory net.ConnectionFactory, version int) Member {
	return &member{ctx, id, factory, version}
}

func (m *member) Id() uuid.UUID {
	return m.id
}

func (m *member) Conn() (net.Connection, error) {
	return m.factory.Conn()
}

func (m *member) Version() int {
	return m.version
}

func (m *member) client() (client, error) {
	conn, err := m.Conn()
	if err != nil {
		return nil, err
	}

	client, err := net.NewClient(m.ctx, conn)
	if err != nil {
		return nil, err
	}

	return newClient(client), nil
}

func (m *member) Write(w enc.Writer) {
	w.Write("id", m.id.String())
	w.Write("connection", m.factory)
	w.Write("version", m.version)
}

type cclient struct {
	client net.Client
}

func newClient(client net.Client) client {
	return &cclient{client: client}
}

func (c *cclient) Close() error {
	return c.client.Close()
}

func (c *cclient) Ping() (bool, error) {
	_, err := c.client.Send(newPingRequest())
	if err != nil {
		return false, errors.New(err)
	}

	return true, nil
}

func (c *cclient) PingProxy(id uuid.UUID) (bool, error) {
	resp, err := c.client.Send(newPingProxyRequest(id))
	if err != nil {
		return false, errors.New(err)
	}

	return readPingProxyResponse(resp)
}

func (c *cclient) Update(updates []update) ([]bool, error) {
	resp, err := c.client.Send(newUpdateRequest(updates))
	if err != nil {
		return nil, errors.New(err)
	}

	return readUpdateResponse(resp)
}

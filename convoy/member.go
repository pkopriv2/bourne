package convoy

import (
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"github.com/pkopriv2/bourne/circuit"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

type member struct {
	id      uuid.UUID
	factory net.ConnectionFactory
	version int
}

func (m *member) Id() uuid.UUID {
	return m.id
}

func (m *member) Version() int {
	return m.version
}

func (m *member) Client() (Client, error) {
	return newClient(m)
}

type client struct {
	member *member
	conn   net.Connection
	enc    *gob.Encoder
	dec    *gob.Decoder
	lock   sync.Mutex
}

func newClient(m *member) (Client, error) {
	conn, err := m.factory()
	if err != nil {
		return nil, err
	}

	return &client{
		member: m, conn: conn, enc: gob.NewEncoder(conn), dec: gob.NewDecoder(conn)}, nil
}

func (c *client) Conn() net.Connection {
	return c.conn
}

func (c *client) Ping(timeout time.Duration) (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	panic("not implemented")
}

func (c *client) ProxyPing(id uuid.UUID, timeout time.Duration) (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	panic("not implemented")
}

func (c *client) Send(u Update, timeout time.Duration) (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var err error
	var resp UpdateResponse
	done, timer := circuit.NewBreaker(timeout, func() {
		err = c.enc.Encode(UpdateRequest{u})
		if err != nil {
			return
		}

		err = c.dec.Decode(&resp)
		if err != nil {
			return
		}
	})

	select {
	case <-done:
		return resp.Accepted, err
	case <-timer:
		return false, circuit.NewTimeoutError(timeout, fmt.Sprintf("Sending update [%v] to member [%v]", u, c.member))
	}
}

type UpdateRequest struct {
	Update Update
}

type UpdateResponse struct {
	Accepted bool
}

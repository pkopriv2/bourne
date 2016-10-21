package convoy

import (
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"github.com/pkopriv2/bourne/concurrent"
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

func (c *client) ping(timeout time.Duration) (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	panic("not implemented")
}

func (c *client) pingProxy(id uuid.UUID, timeout time.Duration) (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	panic("not implemented")
}

func (c *client) update(u Update, timeout time.Duration) (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	ret, timer := concurrent.NewBreaker(timeout, func(res chan<- interface{}) {
		err := c.enc.Encode(UpdateRequest{u})
		if err != nil {
			res <- err
			return
		}

		var resp UpdateResponse
		err = c.dec.Decode(&resp)
		if err != nil {
			res <- err
		}

		res <- resp.Success
	})

	var raw interface{}
	select {
	case <-timer:
		return false, TimeoutError{timeout, fmt.Sprintf("Sending update [%v] to member [%v]", u, c.member)}
	case raw = <-ret:
	}

	switch val := raw.(type) {
	default:
		panic(fmt.Sprintf("Unknown type [%v]", val))
	case error:
		return false, val
	case bool:
		return val, nil
	}
}

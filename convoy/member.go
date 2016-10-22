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

func (m *memberImpl) client() (client, error) {
	return newClient(m)
}

type clientImpl struct {
	member *memberImpl
	conn   net.Connection
	enc    *gob.Encoder
	dec    *gob.Decoder
	lock   sync.Mutex
}

func newClient(m *memberImpl) (client, error) {
	conn, err := m.Conn()
	if err != nil {
		return nil, err
	}

	return &clientImpl{
		member: m, conn: conn, enc: gob.NewEncoder(conn), dec: gob.NewDecoder(conn)}, nil
}

func (c *clientImpl) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.conn.Close()
}

func (c *clientImpl) Ping(timeout time.Duration) (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	ret, timer := concurrent.NewBreaker(timeout, func() interface{} {
		var err error
		if err = c.enc.Encode(PingRequest{}); err != nil {
			return err
		}

		var resp PingResponse

		if err = c.dec.Decode(&resp); err != nil {
			return err
		}

		return true
	})

	var raw interface{}
	select {
	case <-timer:
		return false, TimeoutError{timeout, fmt.Sprintf("Ping [%v] to member [%v]", c.member.id, c.member)}
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

func (c *clientImpl) PingProxy(id uuid.UUID, timeout time.Duration) (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	ret, timer := concurrent.NewBreaker(timeout, func() interface{} {
		var err error
		if err = c.enc.Encode(ProxyPingRequest{id}); err != nil {
			return err
		}

		var resp ProxyPingResponse

		if err = c.dec.Decode(&resp); err != nil {
			return err
		}

		if err = resp.Err; err != nil {
			return err
		}

		return resp.Success
	})

	var raw interface{}
	select {
	case <-timer:
		return false, TimeoutError{timeout, fmt.Sprintf("Proxy ping [%v] to member [%v]", id, c.member)}
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

func (c *clientImpl) Update(u update, timeout time.Duration) (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	ret, timer := concurrent.NewBreaker(timeout, func() interface{} {
		err := c.enc.Encode(UpdateRequest{u})
		if err != nil {
			return err
		}

		var resp UpdateResponse
		err = c.dec.Decode(&resp)
		if err != nil {
			return err
		}

		return resp.Success
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

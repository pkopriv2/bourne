package conn

import (
	"errors"
	"sync"
)

// Implements a simple memory based connection environment.
// This is mostly intended for testing environments, but is exposed
// publicly for general use when necessary.  This was designed
// with the sole purpose of being able to serialize connections
// via connection factories.  Therefore, this requires global
// state in order to manage connections. :(
var MemChannelClosedError = errors.New("Channel closed")
var MemConnectionExistsError = errors.New("Connection exists.")

func MemDial(id int) (Connection, error) {
	return nil, nil
}

func MemListen(id int) (Listener, error) {
	return nil, nil
}

type MemEnvironment interface {
	Create(id int) (Connection, error)
	Listen(id int) (Listener, error)
	CloseConnection(id int) error
	CloseListener(id int) error
}

type memConnectionPair struct {
	dial *memChannel
	recv *memChannel
}

type memEnvironment struct {
	lock      sync.Mutex
	conns     map[int]*memConnectionPair
	listeners map[int]*memListener
}

func (m *memEnvironment) getOrCreate(id int) *memConnectionPair {
	return nil
}

func (m *memEnvironment) Dial(id int) (Connection, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.conns[id] = &memConnectionPair{}
	return nil, nil
}

func (m *memEnvironment) Recv(id int) (Connection, error) {
	panic("not implemented")
}

func (m *memEnvironment) Close(id int) error {
	panic("not implemented")
}

func newMemEnv() MemEnvironment {
	return nil
	// return &memEnv{concurrent.NewMap()}
}

type memListener struct {
	id   int
	env  MemEnvironment
	send *memChannel
	recv *memChannel
}

type MemConnectionFactory struct {
	id int
}

func NewMemConnectionFactory(id int) ConnectionFactory {
	return &MemConnectionFactory{id}
}

func (m *MemConnectionFactory) Conn() (Connection, error) {
	return MemDial(m.id)
}

type memConnection struct {
	id   int
	env  MemEnvironment
	send *memChannel
	recv *memChannel
}

func (m *memConnection) Read(p []byte) (n int, err error) {
	return m.recv.Read(p)
}

func (m *memConnection) Write(p []byte) (n int, err error) {
	return m.send.Write(p)
}

func (m *memConnection) Close() error {
	return m.env.CloseConnection(m.id)
}

type memChannel struct {
	lock  sync.Mutex
	inner chan byte
	close chan struct{}
}

func newmemChannel(close chan struct{}) *memChannel {
	return &memChannel{inner: make(chan byte), close: close}
}

func (m *memChannel) Read(p []byte) (int, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	length := len(p)
	for i := 0; i < length; i++ {
		select {
		case <-m.close:
			return 0, MemChannelClosedError
		case p[i] = <-m.inner:
			continue
		}
	}

	return length, nil
}

func (m *memChannel) Write(p []byte) (n int, err error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	length := len(p)
	for i := 0; i < length; i++ {
		select {
		case <-m.close:
			return 0, MemChannelClosedError
		case m.inner <- byte(p[i]):
			continue
		}
	}

	return length, nil
}

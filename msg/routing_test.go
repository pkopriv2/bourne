package msg

import (
	"testing"
	// "time"

	"github.com/stretchr/testify/assert"
)

// Still need to test concurrency design and failure design.  These
// tests where to ensure that the session addressing scheme would work.

func TestRoutingTable_Listener(t *testing.T) {
	cache := newRoutingTable()
	cache.Add(NewRoutable(NewListenerSession(NewEndPoint(0, 1))))
	assert.NotNil(t, cache.Get(NewListenerSession(NewEndPoint(0, 1))))
}

func TestRoutingTable_Channel(t *testing.T) {
	cache := newRoutingTable()
	cache.Add(NewRoutable(NewChannelSession(NewEndPoint(0, 1), NewEndPoint(0, 1))))
	assert.NotNil(t, cache.Get(NewChannelSession(NewEndPoint(0, 1), NewEndPoint(0, 1))))
}

type testRoutable struct {
	session Session
}

func NewRoutable(session Session) Routable {
	return &testRoutable{session}
}

func (t *testRoutable) Session() Session {
	return t.session
}

func (t *testRoutable) Close() error {
	panic("not implemented")
}

func (t *testRoutable) send(p *packet) error {
	panic("not implemented")
}

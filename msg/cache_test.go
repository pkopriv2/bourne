package msg

import (
	"testing"
	// "time"

	"github.com/stretchr/testify/assert"
)

// Still need to test concurrency design and failure design.  These
// tests where to ensure that the session addressing scheme would work.

func TestCache_Listener(t *testing.T) {
	cache := NewChannelCache()
	cache.Add(NewRoutable(NewListenerSession(NewAddress(0, 1))))
	assert.NotNil(t, cache.Get(NewListenerSession(NewAddress(0, 1))))
}

func TestCache_Channel(t *testing.T) {
	cache := NewChannelCache()
	cache.Add(NewRoutable(NewChannelSession(NewAddress(0, 1), NewAddress(0,1))))
	assert.NotNil(t, cache.Get(NewChannelSession(NewAddress(0, 1), NewAddress(0,1))))
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

func (t *testRoutable) send(p *Packet) error {
	panic("not implemented")
}

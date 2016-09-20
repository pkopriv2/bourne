package client

import (
	"testing"
	// "time"

	"github.com/pkopriv2/bourne/msg/wire"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

// Still need to test concurrency design and failure design.  These
// tests where to ensure that the route addressing scheme would work.

func TestRoutingTable_Listener(t *testing.T) {
	cache := newRoutingTable()

	id := uuid.NewV4()
	route := wire.NewLocalRoute(wire.NewAddress(id, 1))

	cache.Add(NewRoutable(route))

	route = wire.NewLocalRoute(wire.NewAddress(id, 1))
	assert.NotNil(t, cache.Get(route))
}

func TestRoutingTable_Channel(t *testing.T) {
	id1 := uuid.NewV4()
	id2 := uuid.NewV4()
	ep1 := wire.NewAddress(id1, 0)
	ep2 := wire.NewAddress(id2, 1)

	route := wire.NewRemoteRoute(ep1, ep2)

	cache := newRoutingTable()
	cache.Add(NewRoutable(route))

	route = wire.NewRemoteRoute(ep1, ep2)
	assert.NotNil(t, cache.Get(route))
}

type testRoutable struct {
	route wire.Route
}

func NewRoutable(route wire.Route) Routable {
	return &testRoutable{route}
}

func (t *testRoutable) Route() wire.Route {
	return t.route
}

func (t *testRoutable) Close() error {
	panic("not implemented")
}

func (t *testRoutable) send(p wire.Packet) error {
	panic("not implemented")
}

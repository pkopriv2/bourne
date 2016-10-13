package client

import (
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/message/wire"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

// Still need to test concurrency design and failure design.  These
// tests where to ensure that the route addressing scheme would work.

func TestDispatcher_Tx_NoRoute(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())

	memberL := uuid.NewV4()
	memberR := uuid.NewV4()

	addrL := wire.NewAddress(memberL, 0)
	addrR := wire.NewAddress(memberR, 0)
	routeL := wire.NewRemoteRoute(addrL, addrR)

	dispatcher := NewDispatcher(ctx, memberL)

	packet := wire.BuildPacket(routeL.Reverse()).Build()

	dispatcher.Tx() <- packet
	assert.Equal(t, packet, <-dispatcher.Return())

}

func TestDispatcher_Tx(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())

	memberL := uuid.NewV4()
	memberR := uuid.NewV4()
	addrR := wire.NewAddress(memberR, 0)

	dispatcher := NewDispatcher(ctx, memberL)
	tunnelL,_ := dispatcher.NewTunnelSocket(addrR)
	tunnelR,_ := dispatcher.NewTunnelSocket(addrR)

	packet := wire.BuildPacket(tunnelL.Route().Reverse()).Build()
	dispatcher.Tx() <- packet

	select {
	case p := <-tunnelR.Rx():
		assert.Fail(t, "should not have received: ", p)
	case p := <-tunnelL.Rx():
		assert.Equal(t, packet, p)
	}
}

// func TestRoutingTable_Channel(t *testing.T) {
// id1 := uuid.NewV4()
// id2 := uuid.NewV4()
// ep1 := wire.NewAddress(id1, 0)
// ep2 := wire.NewAddress(id2, 1)
//
// route := wire.NewRemoteRoute(ep1, ep2)
//
// cache := newRoutingTable()
// cache.Add(NewRoutable(route))
//
// route = wire.NewRemoteRoute(ep1, ep2)
// assert.NotNil(t, cache.Get(route))
// }
//
// type testRoutable struct {
// route wire.Route
// }
//
// func NewRoutable(route wire.Route) wire.Routable {
// return &testRoutable{route}
// }
//
// func (t *testRoutable) Route() wire.Route {
// return t.route
// }

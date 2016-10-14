package core

import (
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/message/wire"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestDirect_Close_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())

	topo := NewDirectTopology(ctx)
	topo.Close()
}

func TestDirect_Single_Packet_LR(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())

	topo := NewDirectTopology(ctx)
	socketL, _ := topo.SocketL()
	socketR, _ := topo.SocketR()
	defer topo.Close()
	defer socketL.Done()
	defer socketR.Done()

	p := wire.BuildPacket(NewDirectRoute()).Build()
	go func() {
		socketL.Tx() <- p
	}()

	assert.Equal(t, p, <-socketR.Rx())
}

func TestDirect_Single_Packet_RL(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())

	topo := NewDirectTopology(ctx)
	socketL, _ := topo.SocketL()
	socketR, _ := topo.SocketR()
	defer topo.Close()
	defer socketL.Done()
	defer socketR.Done()

	p := wire.BuildPacket(NewDirectRoute()).Build()
	go func() {
		socketR.Tx() <- p
	}()

	assert.Equal(t, p, <-socketL.Rx())
}

func TestDirect_Multi_Packet_RL(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())

	topo := NewDirectTopology(ctx)
	socketL, _ := topo.SocketL()
	socketR, _ := topo.SocketR()
	defer topo.Close()
	defer socketL.Done()
	defer socketR.Done()


	p := wire.BuildPacket(NewDirectRoute()).Build()
	go func() {
		socketR.Tx() <- p
		socketL.Tx() <- p
		socketR.Tx() <- p
		socketL.Tx() <- p
	}()

	assert.Equal(t, p, <-socketL.Rx())
	assert.Equal(t, p, <-socketR.Rx())
	assert.Equal(t, p, <-socketL.Rx())
	assert.Equal(t, p, <-socketR.Rx())
}
func NewDirectRoute() wire.Route {
	return wire.NewRemoteRoute(wire.NewAddress(uuid.NewV4(), 0), wire.NewAddress(uuid.NewV4(), 0))
}

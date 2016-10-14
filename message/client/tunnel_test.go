package client

import (
	"fmt"
	"io"
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/message/core"
	"github.com/pkopriv2/bourne/message/wire"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestTunnel_sendSinglePacket(t *testing.T) {
	fmt.Println("---TestTunnel_sendSinglePacket---")
	control, tunnelL, tunnelR := NewTestTunnelPair()
	defer control.Close()
	defer tunnelL.Close()
	defer tunnelR.Close()

	tunnelR.Write([]byte{1})

	buf := make([]byte, 1024)

	num, err := tunnelL.Read(buf)
	if err != nil {
		t.Fail()
	}

	assert.Equal(t, 1, num)
	assert.Equal(t, []byte{1}, buf[:1])
}

func TestTunnel_sendSingleStream(t *testing.T) {
	fmt.Println("---TestTunnel_sendSingleStream---")
	control, tunnelL, tunnelR := NewTestTunnelPair()
	defer control.Close()
	defer tunnelL.Close()
	defer tunnelR.Close()

	go func() {
		for i := 0; i < 100; i++ {
			tunnelR.Write([]byte{byte(i)})
		}
	}()

	buf := make([]byte, 100)

	num, err := io.ReadFull(tunnelL, buf)
	assert.Nil(t, err)

	assert.Equal(t, 100, num)
	for i := 0; i < 100; i++ {
		assert.Equal(t, byte(i), buf[i])
	}
}

func TestTunnel_sendDuplexStream(t *testing.T) {
	fmt.Println("---TestTunnel_sendDuplexStream---")
	control, tunnelL, tunnelR := NewTestTunnelPair()
	defer control.Close()
	defer tunnelL.Close()
	defer tunnelR.Close()

	go func() {
		for i := 0; i < 100; i++ {
			tunnelR.Write([]byte{byte(i)})
		}
	}()

	go func() {
		for i := 0; i < 100; i++ {
			tunnelL.Write([]byte{byte(i)})
		}
	}()

	bufL := make([]byte, 100)
	bufR := make([]byte, 100)

	numL, errL := io.ReadFull(tunnelL, bufL)
	assert.Nil(t, errL)

	numR, errR := io.ReadFull(tunnelR, bufR)
	assert.Nil(t, errR)

	assert.Equal(t, 100, numL)
	assert.Equal(t, 100, numR)
	for i := 0; i < 100; i++ {
		assert.Equal(t, byte(i), bufL[i])
		assert.Equal(t, byte(i), bufR[i])
	}
}

func TestTunnel_sendSingleLargeStream(t *testing.T) {
	fmt.Println("---TestTunnel_sendSingleLargeStream---")
	control, tunnelL, tunnelR := NewTestTunnelPair()
	defer control.Close()
	defer tunnelL.Close()
	defer tunnelR.Close()

	buf := make([]byte, 4096)
	for i := 0; i < 4096; i++ {
		buf[i] = byte(i)
	}

	go func() {
		for i := 0; i < 1<<12; i++ {
			tunnelR.Write(buf)
		}
	}()

	actual := make([]byte, 4096)

	for i := 0; i < 1<<12; i++ {
		num, _ := io.ReadFull(tunnelL, actual)
		// assert.Nil(t, err)

		assert.Equal(t, 4096, num)
		// for i := 0; i < 1024; i++ {
		// assert.Equal(t, byte(i), actual[i])
		// }
	}
}

func NewTestTunnelPair() (core.DirectTopology, Tunnel, Tunnel) {
	lId := uuid.NewV4()
	rId := uuid.NewV4()

	routeL := wire.NewRemoteRoute(wire.NewAddress(lId, 0), wire.NewAddress(rId, 0))
	routeR := routeL.Reverse()

	topo := core.NewDirectTopology(common.NewContext(common.NewEmptyConfig()))
	socketL, _ := topo.SocketL()
	socketR, _ := topo.SocketR()

	tunnelL := NewTunnel(&TestTunnelSocket{topo.Context(), routeL, false, socketL})
	tunnelR := NewTunnel(&TestTunnelSocket{topo.Context(), routeR, true, socketR})
	return topo, tunnelL, tunnelR
}

type TestTunnelSocket struct {
	ctx       common.Context
	route     wire.Route
	listening bool
	socket    core.StandardSocket
}

func (t *TestTunnelSocket) Closed() <-chan struct{} {
	return t.socket.Closed()
}

func (t *TestTunnelSocket) Failed() <-chan struct{} {
	return t.socket.Failed()
}

func (t *TestTunnelSocket) Failure() error {
	return t.socket.Failure()
}

func (t *TestTunnelSocket) Done() {
	t.socket.Done()
}

func (t *TestTunnelSocket) Rx() <-chan wire.Packet {
	return t.socket.Rx()
}

func (t *TestTunnelSocket) Tx() chan<- wire.Packet {
	return t.socket.Tx()
}

func (t *TestTunnelSocket) Listening() bool {
	return t.listening
}

func (t *TestTunnelSocket) Route() wire.Route {
	return t.route
}

func (t *TestTunnelSocket) Context() common.Context {
	return t.ctx
}

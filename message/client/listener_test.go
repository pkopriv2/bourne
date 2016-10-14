package client

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/message/core"
	"github.com/pkopriv2/bourne/message/wire"
)

// func TestTunnel_sendSinglePacket(t *testing.T) {
// fmt.Println("---TestTunnel_sendSinglePacket---")
// _, tunnelL, tunnelR := NewTestTunnelPair()
// defer tunnelL.Close()
//
// tunnelR.Write([]byte{1})
//
// buf := make([]byte, 1024)
//
// num, err := tunnelL.Read(buf)
// if err != nil {
// t.Fail()
// }
//
// assert.Equal(t, 1, num)
// assert.Equal(t, []byte{1}, buf[:1])
// }

// func NewTestListener() (core.DirectTopology, Listener) {
// memberId := uuid.NewV4()
//
// := wire.NewLocalRoute(wire.NewAddress(memberId, 0))
//
// topo := core.NewDirectTopology(common.NewContext(common.NewEmptyConfig()))
//
// tunnelL := NewTunnel(&TestTunnelSocket{topo.Context(), routeL, false, socketL})
// tunnelR := NewTunnel(&TestTunnelSocket{topo.Context(), routeR, true, socketR})
// return topo, tunnelL, tunnelR
// }

type TestListenerSocket struct {
	ctx    common.Context
	route  wire.Route
	socket core.StandardSocket
}

// func (t *TestListenerSocket) Closed() <-chan struct{} {
// panic("not implemented")
// }
//
// func (t *TestListenerSocket) Failed() <-chan struct{} {
// panic("not implemented")
// }
//
// func (t *TestListenerSocket) Failure() error {
// panic("not implemented")
// }
//
// func (t *TestListenerSocket) Done() {
// panic("not implemented")
// }
//
// func (t *TestListenerSocket) Rx() <-chan wire.Packet {
// panic("not implemented")
// }
//
// func (t *TestListenerSocket) Tx() chan<- wire.Packet {
// panic("not implemented")
// }
//
// func (t *TestListenerSocket) Route() wire.Route {
// panic("not implemented")
// }
//
// func (t *TestListenerSocket) Context() common.Context {
// panic("not implemented")
// }
//
// func (t *TestListenerSocket) NewTunnel(wire.Address) (client.TunnelSocket, error) {
// panic("not implemented")
// }

package tunnel

import (
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/machine"
	"github.com/pkopriv2/bourne/message/wire"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestRecvMain_SingleVerify(t *testing.T) {
	driver, m := NewTestReceiver()
	defer m.Close()

	driver.PacketRx <- wire.BuildPacket(NewRoute()).SetVerify(100).Build()
	assert.Equal(t, wire.NewNumMessage(100), <-driver.VerifyTx)
}

func TestRecvMain_SingleSegment(t *testing.T) {
	driver, m := NewTestReceiver()
	defer m.Close()

	driver.PacketRx <- wire.BuildPacket(NewRoute()).SetSegment(100, []byte{0, 1, 2}).Build()
	assert.Equal(t, wire.NewSegmentMessage(100, []byte{0, 1, 2}), <-driver.SegmentTx)
}

func TestRecvMain_SegmentAndVerify(t *testing.T) {
	driver, m := NewTestReceiver()
	defer m.Close()

	driver.PacketRx <- wire.BuildPacket(NewRoute()).SetVerify(99).SetSegment(100, []byte{0, 1, 2}).Build()

	assert.Equal(t, wire.NewSegmentMessage(100, []byte{0, 1, 2}), <-driver.SegmentTx)
	assert.Equal(t, wire.NewNumMessage(99), <-driver.VerifyTx)
}

func TestRecvMain_Close(t *testing.T) {
	driver, m := NewTestReceiver()
	m.Close()

	builder := m.Clone()
	builder.AddState(TunnelClosingRecv, func(_ machine.WorkerSocket, _ []interface{}) {})

	m = builder.Start(TunnelOpened)

	driver.PacketRx <- wire.BuildPacket(NewRoute()).SetClose(100).Build()

	assert.Nil(t, m.Wait())
	assert.Equal(t, machine.NewState(TunnelOpened), machine.ExtractNthState(m, 0))
	assert.Equal(t, machine.NewState(TunnelClosingRecv, uint64(100)), machine.ExtractNthState(m, 1))
}

func TestRecvMain_Error(t *testing.T) {
	driver, m := NewTestReceiver()
	defer m.Close()

	err := wire.NewProtocolErrorFamily(1)("msg")
	driver.PacketRx <- wire.BuildPacket(NewRoute()).SetError(err).Build()

	assert.Equal(t, err, m.Wait())
}

type RecvMainDriver struct {
	PacketRx  chan wire.Packet
	SegmentTx chan wire.SegmentMessage
	VerifyTx  chan wire.NumMessage
}

func NewRecvMainDriver() *RecvMainDriver {
	return &RecvMainDriver{make(chan wire.Packet), make(chan wire.SegmentMessage), make(chan wire.NumMessage)}
}

func (s *RecvMainDriver) NewRecvMainSocket() *ReceiverSocket {
	return &ReceiverSocket{s.PacketRx, s.SegmentTx, s.VerifyTx}
}

func NewTestReceiver() (*RecvMainDriver, machine.StateMachine) {
	ctx := common.NewContext(common.NewEmptyConfig())

	driver := NewRecvMainDriver()

	builder := machine.NewStateMachine()
	builder.AddState(TunnelOpened, NewReceiver(ctx, driver.NewRecvMainSocket()))
	machine := builder.Start(TunnelOpened)
	return driver, machine
}

func NewRoute() wire.Route {
	return wire.NewRemoteRoute(wire.NewAddress(uuid.NewV4(), 0), wire.NewAddress(uuid.NewV4(), 0))
}

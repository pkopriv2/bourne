package tunnel

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/machine"
	"github.com/pkopriv2/bourne/message/wire"
	"github.com/stretchr/testify/assert"
)

func TestSendMain_Timeout(t *testing.T) {

	// ugh... maybe an actual class would be better....
	_, driver, stream, machine := NewTestSender()
	defer machine.Close()

	// need to write before we select on any channels
	stream.Write([]byte{1})
	<-driver.PacketTx

	assert.NotNil(t, machine.Wait())
}

func TestSendMain_SingleTimeout(t *testing.T) {
	// ugh... maybe an actual class would be better....
	_, driver, stream, machine := NewTestSender()
	defer machine.Close()

	stream.Write([]byte{1})

	p1 := <-driver.PacketTx
	p2 := <-driver.PacketTx
	assert.Equal(t, p1, p2)
}

func TestSendMain_SingleSendVerify(t *testing.T) {
	_, driver, stream, machine := NewTestSender()
	defer machine.Close()

	stream.Write([]byte{1})

	p := <-driver.PacketTx
	driver.SendVerifyRx <- wire.NewNumMessage(1)

	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, p.Segment(), wire.NewSegmentMessage(0, []byte{1}))
	assert.Equal(t, []byte{}, stream.Data())
}

func TestSendMain_SingleRecvVerify(t *testing.T) {
	route, driver, stream, machine := NewTestSender()
	defer machine.Close()

	stream.Write([]byte{1})
	driver.RecvVerifyRx <- wire.NewNumMessage(1)
	assert.Equal(t, wire.BuildPacket(route).SetVerify(1).Build(), <-driver.PacketTx)
}

type SendMainDriver struct {
	PacketTx     chan wire.Packet
	SendVerifyRx chan wire.NumMessage
	RecvVerifyRx chan wire.NumMessage
}

func newSendMainDriver() *SendMainDriver {
	return &SendMainDriver{make(chan wire.Packet), make(chan wire.NumMessage), make(chan wire.NumMessage)}
}

func (s *SendMainDriver) NewSendMainSocket() *SenderSocket {
	return &SenderSocket{s.PacketTx, s.SendVerifyRx, s.RecvVerifyRx}
}

func NewTestSender() (wire.Route, *SendMainDriver, *concurrent.Stream, machine.StateMachine) {
	conf := common.NewConfig(map[string]interface{}{confTunnelVerifyTimeout: 100})
	ctx := common.NewContext(conf)

	route := NewRoute()
	driver := newSendMainDriver()
	stream := concurrent.NewStream(10)

	builder := machine.NewStateMachine()
	builder.AddState(TunnelOpened, NewSender(route, ctx, stream, driver.NewSendMainSocket()))
	return route, driver, stream, builder.Start(TunnelOpened)
}

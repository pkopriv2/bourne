package tunnel

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestSendMain_SingleSendVerify(t *testing.T) {
	env := newTunnelEnv(utils.NewEmptyConfig())

	sendMain := make(chan wire.Packet)
	channels := &tunnelChannels{
		recvVerifier: make(chan wire.NumMessage),
		sendVerifier: make(chan wire.NumMessage),
		sendMain:     sendMain}

	l := wire.NewAddress(uuid.NewV4(), 0)
	r := wire.NewAddress(uuid.NewV4(), 0)
	route := wire.NewRemoteRoute(l, r)
	stream, worker := NewSendMain(route, env, channels)

	stream.Write([]byte{1})
	stream.TryRead([]byte{0}, false)

	builder := utils.BuildStateMachine()
	builder.AddState(1, worker)

	machine := builder.Start(1)
	defer utils.Terminate(machine)

	assert.Equal(t, []byte{1}, stream.Data())
	channels.sendVerifier <- wire.NewNumMessage(0)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, []byte{}, stream.Data())
}

func TestSendMain_SingleSegment(t *testing.T) {
	env := newTunnelEnv(utils.NewEmptyConfig())

	sendMain := make(chan wire.Packet)
	channels := &tunnelChannels{
		sendVerifier: make(chan wire.NumMessage),
		sendMain:     sendMain}

	l := wire.NewAddress(uuid.NewV4(), 0)
	r := wire.NewAddress(uuid.NewV4(), 0)
	route := wire.NewRemoteRoute(l, r)
	stream, worker := NewSendMain(route, env, channels)

	builder := utils.BuildStateMachine()
	builder.AddState(1, worker)

	machine := builder.Start(1)
	defer utils.Terminate(machine)

	num, err := stream.Write([]byte{0})
	assert.Equal(t, 1, num)
	assert.Nil(t, err)

	assert.Equal(t, wire.BuildPacket(route).SetSegment(0, []byte{0}).Build(), <-sendMain)
}

func TestSendMain_SingleRecvVerify(t *testing.T) {
	env := newTunnelEnv(utils.NewEmptyConfig())

	sendMain := make(chan wire.Packet)
	channels := &tunnelChannels{
		recvVerifier: make(chan wire.NumMessage),
		sendVerifier: make(chan wire.NumMessage),
		sendMain:     sendMain}

	l := wire.NewAddress(uuid.NewV4(), 0)
	r := wire.NewAddress(uuid.NewV4(), 0)
	route := wire.NewRemoteRoute(l, r)
	_, worker := NewSendMain(route, env, channels)

	builder := utils.BuildStateMachine()
	builder.AddState(1, worker)

	machine := builder.Start(1)
	defer utils.Terminate(machine)

	channels.recvVerifier <- wire.NewNumMessage(1)
	assert.Equal(t, wire.BuildPacket(route).SetVerify(1).Build(), <-sendMain)
}

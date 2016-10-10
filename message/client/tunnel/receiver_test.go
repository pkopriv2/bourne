package tunnel

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestRecvMain_Close(t *testing.T) {
	env := newTunnelEnv(utils.NewEmptyConfig())

	recvMain := make(chan wire.Packet)
	channels := &tunnelChannels{
		assemblerIn:  make(chan wire.SegmentMessage),
		sendVerifier: make(chan wire.NumMessage),
		mainIn:       recvMain}

	worker := NewRecvMain(env, channels)

	builder := utils.BuildStateMachine()
	builder.AddState(1, worker)
	builder.AddState(TunnelClosingRecv, func(_ utils.WorkerController, _ []interface{}) {})

	machine := builder.Start(1)
	defer utils.Terminate(machine)

	controller := machine.Control()

	l := wire.NewAddress(uuid.NewV4(), 0)
	r := wire.NewAddress(uuid.NewV4(), 0)
	p := wire.BuildPacket(wire.NewRemoteRoute(l, r)).SetClose(100).Build()

	done, timeout := utils.NewCircuitBreaker(time.Second, func() { recvMain <- p })
	select {
	case <-done:
	case <-timeout:
		t.Fail()
		return
	}

	assert.Nil(t, <-controller.Wait())
	assert.Equal(t, utils.State(1), controller.Summary()[0])
	assert.Equal(t, utils.State(TunnelClosingRecv, uint64(100)), controller.Summary()[1])
	assert.Equal(t, utils.Terminal(), controller.Summary()[2])
}

func TestRecvMain_Error(t *testing.T) {
	env := newTunnelEnv(utils.NewEmptyConfig())

	recvMain := make(chan wire.Packet)
	channels := &tunnelChannels{
		assemblerIn:  make(chan wire.SegmentMessage),
		sendVerifier: make(chan wire.NumMessage),
		mainIn:       recvMain}

	worker := NewRecvMain(env, channels)

	builder := utils.BuildStateMachine()
	builder.AddState(1, worker)
	builder.AddState(TunnelClosingRecv, func(_ utils.WorkerController, _ []interface{}) {})

	machine := builder.Start(1)
	defer utils.Terminate(machine)

	controller := machine.Control()

	err := wire.NewProtocolErrorFamily(1)("msg")

	l := wire.NewAddress(uuid.NewV4(), 0)
	r := wire.NewAddress(uuid.NewV4(), 0)
	p := wire.BuildPacket(wire.NewRemoteRoute(l, r)).SetError(err).Build()

	done, timeout := utils.NewCircuitBreaker(time.Second, func() { recvMain <- p })
	select {
	case <-done:
	case <-timeout:
		t.Fail()
		return
	}

	assert.Equal(t, err, <-controller.Wait())
	assert.Equal(t, []utils.Transition{utils.State(1), utils.Fail(err)}, controller.Summary())
}

func TestRecvMain_SingleVerify(t *testing.T) {
	env := newTunnelEnv(utils.NewEmptyConfig())

	recvMain := make(chan wire.Packet)
	channels := &tunnelChannels{
		assemblerIn:  make(chan wire.SegmentMessage),
		sendVerifier: make(chan wire.NumMessage),
		mainIn:       recvMain}

	worker := NewRecvMain(env, channels)

	builder := utils.BuildStateMachine()
	builder.AddState(1, worker)
	builder.AddState(TunnelClosingRecv, func(_ utils.WorkerController, _ []interface{}) {})

	machine := builder.Start(1)
	defer utils.Terminate(machine)

	l := wire.NewAddress(uuid.NewV4(), 0)
	r := wire.NewAddress(uuid.NewV4(), 0)
	p := wire.BuildPacket(wire.NewRemoteRoute(l, r)).SetVerify(100).Build()

	done, timeout := utils.NewCircuitBreaker(time.Second, func() { recvMain <- p })
	select {
	case <-done:
	case <-timeout:
		t.Fail()
		return
	}

	assert.Equal(t, wire.NewNumMessage(100), <-channels.sendVerifier)
}

func TestRecvMain_SingleSegment(t *testing.T) {
	env := newTunnelEnv(utils.NewEmptyConfig())

	recvMain := make(chan wire.Packet)
	channels := &tunnelChannels{
		assemblerIn:  make(chan wire.SegmentMessage),
		sendVerifier: make(chan wire.NumMessage),
		mainIn:       recvMain}

	worker := NewRecvMain(env, channels)

	builder := utils.BuildStateMachine()
	builder.AddState(1, worker)
	builder.AddState(TunnelClosingRecv, func(_ utils.WorkerController, _ []interface{}) {})

	machine := builder.Start(1)
	defer utils.Terminate(machine)

	l := wire.NewAddress(uuid.NewV4(), 0)
	r := wire.NewAddress(uuid.NewV4(), 0)
	p := wire.BuildPacket(wire.NewRemoteRoute(l, r)).SetSegment(100, []byte{0, 1, 2}).Build()

	done, timeout := utils.NewCircuitBreaker(time.Second, func() { recvMain <- p })
	select {
	case <-done:
	case <-timeout:
		t.Fail()
		return
	}

	assert.Equal(t, wire.NewSegmentMessage(100, []byte{0, 1, 2}), <-channels.assemblerIn)
}

func TestRecvMain_SegmentAndVerify(t *testing.T) {
	env := newTunnelEnv(utils.NewEmptyConfig())

	recvMain := make(chan wire.Packet)
	channels := &tunnelChannels{
		assemblerIn:  make(chan wire.SegmentMessage),
		sendVerifier: make(chan wire.NumMessage),
		mainIn:       recvMain}

	worker := NewRecvMain(env, channels)

	builder := utils.BuildStateMachine()
	builder.AddState(1, worker)
	builder.AddState(TunnelClosingRecv, func(_ utils.WorkerController, _ []interface{}) {})

	machine := builder.Start(1)
	defer utils.Terminate(machine)

	l := wire.NewAddress(uuid.NewV4(), 0)
	r := wire.NewAddress(uuid.NewV4(), 0)
	p := wire.BuildPacket(wire.NewRemoteRoute(l, r)).SetVerify(200).SetSegment(100, []byte{0, 1, 2}).Build()

	done, timeout := utils.NewCircuitBreaker(time.Second, func() { recvMain <- p })
	select {
	case <-done:
	case <-timeout:
		t.Fail()
		return
	}

	assert.Equal(t, wire.NewSegmentMessage(100, []byte{0, 1, 2}), <-channels.assemblerIn)

	// this should timeout if properly working
	done, timeout = utils.NewCircuitBreaker(time.Second, func() { recvMain <- p })
	select {
	case <-done:
		t.Fail()
		return
	case <-timeout:
	}

	assert.Equal(t, wire.NewNumMessage(200), <-channels.sendVerifier)
}

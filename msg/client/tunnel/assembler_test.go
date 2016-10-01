package tunnel

import (
	// "io"

	"testing"

	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
	"github.com/stretchr/testify/assert"
)

func TestPendingSegments_Next_Empty(t *testing.T) {
	pend := NewPendingSegments(10)
	assert.Nil(t, pend.Next())
}

func TestPendingSegments_Next_SingleItem(t *testing.T) {
	pend := NewPendingSegments(10)
	pend.Add(0, []byte{0})
	assert.Equal(t, []byte{0}, pend.Next())
}

func TestPendingSegments_Next_SingleItem_OutOfOrder(t *testing.T) {
	pend := NewPendingSegments(10)
	pend.Add(1, []byte{0})
	assert.Nil(t, pend.Next())
}

func TestPendingSegments_Next_MultipleItems_Gap(t *testing.T) {
	pend := NewPendingSegments(10)
	pend.Add(0, []byte{0})
	pend.Add(2, []byte{2})
	assert.Equal(t, []byte{0}, pend.Next())
	assert.Nil(t, pend.Next())
}

func TestPendingSegments_Next_MultipleItems_Duplicate(t *testing.T) {
	pend := NewPendingSegments(10)
	pend.Add(0, []byte{0, 1, 2})
	pend.Add(1, []byte{1})
	assert.Equal(t, []byte{0, 1, 2}, pend.Next())
	assert.Nil(t, pend.Next())
}

func TestPendingSegments_Take_Empty(t *testing.T) {
	pend := NewPendingSegments(10)
	assert.Nil(t, pend.Take())
}

func TestPendingSegments_Take_SingleItem(t *testing.T) {
	pend := NewPendingSegments(10)
	pend.Add(0, []byte{0})
	assert.Equal(t, []byte{0}, pend.Take())
}

func TestPendingSegments_Take_MultipleItems_Contiguous(t *testing.T) {
	pend := NewPendingSegments(10)
	pend.Add(0, []byte{0, 1})
	pend.Add(2, []byte{2})
	pend.Add(4, []byte{4})
	assert.Equal(t, []byte{0, 1, 2}, pend.Take())
	assert.Nil(t, pend.Take())
}

func TestAssembler_SingleByte(t *testing.T) {
	env := newTunnelEnv(utils.NewEmptyConfig())

	channels := &tunnelChannels{
		assembler:    make(chan wire.SegmentMessage),
		recvVerifier: make(chan wire.NumMessage),
		bufferer:     make(chan []byte)}

	builder := utils.BuildStateMachine()
	builder.AddState(1, NewRecvAssembler(env, channels))

	machine := builder.Start(1)
	defer utils.Terminate(machine)

	channels.assembler <- wire.NewSegmentMessage(0, []byte{0})
	assert.Equal(t, wire.NewNumMessage(1), <-channels.recvVerifier)
	assert.Equal(t, []byte{0}, <-channels.bufferer)
}

func TestAssembler_MultiByte(t *testing.T) {
	env := newTunnelEnv(utils.NewEmptyConfig())

	channels := &tunnelChannels{
		assembler:    make(chan wire.SegmentMessage),
		recvVerifier: make(chan wire.NumMessage),
		bufferer:     make(chan []byte)}

	builder := utils.BuildStateMachine()
	builder.AddState(1, NewRecvAssembler(env, channels))

	machine := builder.Start(1)
	defer utils.Terminate(machine)

	channels.assembler <- wire.NewSegmentMessage(0, []byte{0,1})
	assert.Equal(t, wire.NewNumMessage(2), <-channels.recvVerifier)
	assert.Equal(t, []byte{0,1}, <-channels.bufferer)
}

func TestAssembler_OutOfOrder(t *testing.T) {
	env := newTunnelEnv(utils.NewEmptyConfig())

	channels := &tunnelChannels{
		assembler:    make(chan wire.SegmentMessage),
		recvVerifier: make(chan wire.NumMessage),
		bufferer:     make(chan []byte)}

	builder := utils.BuildStateMachine()
	builder.AddState(1, NewRecvAssembler(env, channels))

	machine := builder.Start(1)
	defer utils.Terminate(machine)

	channels.assembler <- wire.NewSegmentMessage(0, []byte{0,1})
	channels.assembler <- wire.NewSegmentMessage(3, []byte{3,4,5})
	channels.assembler <- wire.NewSegmentMessage(2, []byte{2})
	assert.Equal(t, wire.NewNumMessage(2), <-channels.recvVerifier)
	assert.Equal(t, []byte{0,1}, <-channels.bufferer)
	assert.Equal(t, []byte{2,3,4,5}, <-channels.bufferer)
	assert.Equal(t, wire.NewNumMessage(6), <-channels.recvVerifier)
}

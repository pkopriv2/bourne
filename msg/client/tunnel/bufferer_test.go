package tunnel

import (
	"io"
	"testing"

	"github.com/pkopriv2/bourne/utils"
	"github.com/stretchr/testify/assert"
)

func TestRecvBuffer_Close(t *testing.T) {
	env := newTunnelEnv(utils.NewEmptyConfig())

	channels := &tunnelChannels{
		bufferer: make(chan []byte)}

	stream, worker := NewRecvBuffer(env, channels)
	machine := utils.BuildStateMachine().AddState(1, worker).Start(1)

	// immediately terminate the machine.
	utils.Terminate(machine)

	go func() {
		for i := 0; i < 100; i++ {
			channels.bufferer <- []byte{byte(i)}
		}
	}()

	// just take a couple passes to make sure it stays empty
	for i := 0; i < 100; i++ {
		assert.Equal(t, []byte{}, stream.Data())
	}
}

func TestRecvBuffer(t *testing.T) {
	env := newTunnelEnv(utils.NewEmptyConfig())

	channels := &tunnelChannels{
		bufferer: make(chan []byte)}

	stream, worker := NewRecvBuffer(env, channels)
	machine := utils.BuildStateMachine().AddState(1, worker).Start(1)
	defer utils.Terminate(machine)

	go func() {
		for i := 0; i < 100; i++ {
			channels.bufferer <- []byte{byte(i)}
		}
	}()

	buf := make([]byte, 100)
	num, err := io.ReadFull(stream, buf)

	assert.Nil(t, err)
	assert.Equal(t, 100, num)

	for i := 0; i < 100; i++ {
		assert.Equal(t, byte(i), buf[i])
	}
}

package tunnel

import (
	"io"
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/machine"
	"github.com/stretchr/testify/assert"
)

func TestRecvBuffer_Close(t *testing.T) {
	tx, stream, machine := NewTestBufferer()

	// kill the machine
	machine.Close()
	time.Sleep(10 * time.Millisecond)

	go func() {
		for i := 0; i < 100; i++ {
			tx <- []byte{byte(i)}
		}
	}()

	// just take a couple passes to make sure it stays empty
	for i := 0; i < 100; i++ {
		assert.Equal(t, []byte{}, stream.Data())
	}
}

func TestRecvBuffer_Simple(t *testing.T) {
	tx, stream, machine := NewTestBufferer()
	defer machine.Close()

	go func() {
		for i := 0; i < 100; i++ {
			tx <- []byte{byte(i)}
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

type BuffererDriver struct {
	SegmentTx chan []byte
}

func newBuffererDriver() *BuffererDriver {
	return &BuffererDriver{make(chan []byte)}
}

func (b *BuffererDriver) NewSocket() *BuffererSocket {
	return &BuffererSocket{b.SegmentTx}
}

func NewTestBufferer() (chan<- []byte, *concurrent.Stream, machine.StateMachine) {
	ctx := common.NewContext(common.NewEmptyConfig())

	driver := newBuffererDriver()
	stream := concurrent.NewStream(10)

	builder := machine.NewStateMachine()
	builder.AddState(TunnelOpened, NewBufferer(ctx, driver.NewSocket(), stream))
	return driver.SegmentTx, stream, builder.Start(TunnelOpened)
}

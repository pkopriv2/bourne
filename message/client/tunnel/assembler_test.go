package tunnel

import (
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/machine"
	"github.com/pkopriv2/bourne/message/wire"
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
	driver, machine := NewTestAssembler()
	defer machine.Close()

	driver.SegmentRx <- wire.NewSegmentMessage(0, []byte{0})
	assert.Equal(t, wire.NewNumMessage(1), <-driver.VerifyTx)
	assert.Equal(t, []byte{0}, <-driver.SegmentTx)
}

func TestAssembler_MultiByte(t *testing.T) {
	driver, machine := NewTestAssembler()
	defer machine.Close()

	driver.SegmentRx <- wire.NewSegmentMessage(0, []byte{0, 1})
	assert.Equal(t, wire.NewNumMessage(2), <-driver.VerifyTx)
	assert.Equal(t, []byte{0, 1}, <-driver.SegmentTx)
}

func TestAssembler_OutOfOrder(t *testing.T) {
	driver, machine := NewTestAssembler()
	defer machine.Close()

	driver.SegmentRx <- wire.NewSegmentMessage(0, []byte{0, 1})
	driver.SegmentRx <- wire.NewSegmentMessage(3, []byte{3, 4, 5})
	driver.SegmentRx <- wire.NewSegmentMessage(2, []byte{2})

	assert.Equal(t, wire.NewNumMessage(2), <-driver.VerifyTx)
	assert.Equal(t, []byte{0, 1}, <-driver.SegmentTx)
	assert.Equal(t, []byte{2, 3, 4, 5}, <-driver.SegmentTx)
	assert.Equal(t, wire.NewNumMessage(6), <-driver.VerifyTx)

}

type AssemblerDriver struct {
	SegmentRx chan wire.SegmentMessage
	SegmentTx chan []byte
	VerifyTx  chan wire.NumMessage
}

func newAssemblerDriver() *AssemblerDriver {
	return &AssemblerDriver{make(chan wire.SegmentMessage), make(chan []byte), make(chan wire.NumMessage)}
}

func (s *AssemblerDriver) NewAssemblerSocket() *AssemblerSocket {
	return &AssemblerSocket{s.SegmentRx, s.SegmentTx, s.VerifyTx}
}

func NewTestAssembler() (*AssemblerDriver, machine.StateMachine) {
	ctx := common.NewContext(common.NewEmptyConfig())

	driver := newAssemblerDriver()

	builder := machine.NewStateMachine()
	builder.AddState(1, NewAssembler(ctx, driver.NewAssemblerSocket()))
	machine := builder.Start(1)

	return driver, machine
}

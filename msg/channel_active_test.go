package msg

import "testing"

//import "bytes"
//import "bufio"
// import "io"
import "fmt"

// import "time"
// import "sync"

import "github.com/stretchr/testify/assert"

//import "github.com/pkopriv2/bourne/msg"

func TestActiveChannel(t *testing.T) {

	assert.Equal(t, 123, 123, "they should be equal")
	fmt.Println("hello")
}

// func TestRoundUp_small(t *testing.T) {
	// assert.Equal(t, uint32(1), roundUp(1))
	// assert.Equal(t, uint32(2), roundUp(2))
	// assert.Equal(t, uint32(4), roundUp(3))
	// assert.Equal(t, uint32(4), roundUp(4))
	// assert.Equal(t, uint32(8), roundUp(5))
// }
//
// func TestSendBuffer_init(t *testing.T) {
	// buf := NewSendBuffer(8)
//
	// assert.Equal(t, []byte{}, buf.Data())
	// assert.Equal(t, uint32(0), buf.SeqPos())
	// assert.Equal(t, uint32(0), buf.AckPos())
// }
//
// func TestSendBuffer_AddOne(t *testing.T) {
	// buf := NewSendBuffer(8)
	// assert.Equal(t, uint32(1), buf.Add([]byte{1}))
	// assert.Equal(t, []byte{1}, buf.Data())
	// assert.Equal(t, uint32(1), buf.SeqPos())
	// assert.Equal(t, uint32(0), buf.AckPos())
// }
//
// func TestSendBuffer_AddOne_AckOne(t *testing.T) {
	// buf := NewSendBuffer(8)
	// assert.Equal(t, uint32(1), buf.Add([]byte{1}))
	// assert.Equal(t, nil, buf.Ack(uint32(1)))
//
	// assert.Equal(t, uint32(1), buf.SeqPos())
	// assert.Equal(t, uint32(1), buf.AckPos())
// }
//
// func TestSendBuffer_invalidAck(t *testing.T) {
	// buf := NewSendBuffer(4)
	// assert.Equal(t, uint32(1), buf.Add([]byte{1}))
	// assert.Equal(t, nil, buf.Ack(uint32(1)))
	// assert.Equal(t, ERR_CHANNEL_INVALID_ACK, buf.Ack(uint32(1)))
// }

func TestSendBuffer_wrap(t *testing.T) {
	buf := NewSendBuffer(4)

	assert.Equal(t, uint32(4), buf.Add([]byte{1,2,3,4}))
	assert.Equal(t, []byte{4,1,2,3}, buf.data)
	assert.Equal(t, []byte{1,2,3,4}, buf.Data())
	assert.Equal(t, uint32(4), buf.SeqPos())
	assert.Equal(t, uint32(0), buf.AckPos())

}


//
// func TestSendBuffer_addNoWrap(t *testing.T) {
// buf := NewSendBuffer(8)
// buf.Add([]byte{1,2})
//
// assert.Equal(t, 2, buf.Seq())
// assert.Equal(t, 0, buf.Ack())
// assert.Equal(t, []byte{1,2}, buf.Data())
// }

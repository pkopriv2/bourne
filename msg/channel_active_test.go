package msg

import (
	"sync"
	"testing"
	"time"
	"io"

	"github.com/stretchr/testify/assert"
)


func TestRingBuffer_init(t *testing.T) {
	buf := NewRingBuffer(8)

	assert.Equal(t, []byte{}, buf.Data())
	assert.Equal(t, uint32(0), buf.WritePos())
	assert.Equal(t, uint32(0), buf.ReadPos())
}

func TestRingBuffer_TryWrite_One(t *testing.T) {
	buf := NewRingBuffer(4)
	assert.Equal(t, uint32(1), buf.tryWrite([]byte{1}))
	assert.Equal(t, []byte{1}, buf.Data())
	assert.Equal(t, uint32(1), buf.WritePos())
	assert.Equal(t, uint32(0), buf.ReadPos())
}

func TestRingBuffer_TryWrite_ToCapacity(t *testing.T) {
	buf := NewRingBuffer(4)

	assert.Equal(t, uint32(4), buf.tryWrite([]byte{1, 2, 3, 4}))
	assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())
	assert.Equal(t, uint32(4), buf.WritePos())
	assert.Equal(t, uint32(0), buf.ReadPos())
}

func TestRingBuffer_TryWrite_BeyondCapacity(t *testing.T) {
	buf := NewRingBuffer(4)

	assert.Equal(t, uint32(3), buf.tryWrite([]byte{1, 2, 3}))
	assert.Equal(t, []byte{1, 2, 3}, buf.Data())
	assert.Equal(t, uint32(3), buf.WritePos())
	assert.Equal(t, uint32(0), buf.ReadPos())

	assert.Equal(t, uint32(1), buf.tryWrite([]byte{4, 5, 6}))
	assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())
	assert.Equal(t, uint32(4), buf.WritePos())
	assert.Equal(t, uint32(0), buf.ReadPos())
}

func TestRingBuffer_Write_LessThanCapacity(t *testing.T) {
	buf := NewRingBuffer(4)

	num, err := buf.Write([]byte{1})
	assert.Equal(t, 1, num)
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte{1}, buf.Data())
	assert.Equal(t, uint32(1), buf.WritePos())
	assert.Equal(t, uint32(0), buf.ReadPos())
}

func TestRingBuffer_Write_EqualToCapacity(t *testing.T) {
	buf := NewRingBuffer(4)

	num, err := buf.Write([]byte{1, 2, 3, 4})
	assert.Equal(t, 4, num)
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())
	assert.Equal(t, uint32(4), buf.WritePos())
	assert.Equal(t, uint32(0), buf.ReadPos())
}

func TestRingBuffer_Write_GreaterCapacity(t *testing.T) {
	buf := NewRingBuffer(4)
	var writeEnd time.Time

	wait := new(sync.WaitGroup)
	wait.Add(1)
	go func() {
		buf.Write([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})
		writeEnd = time.Now()
		wait.Done()
	}()

	time.Sleep(100)
	readBeg := time.Now()

	readBuf := make([]byte, 9)
	io.ReadFull(buf, readBuf)

	wait.Wait()
	assert.True(t, writeEnd.After(readBeg))
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, readBuf)
}

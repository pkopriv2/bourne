package msg

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLogBuffer_init(t *testing.T) {
	buf := NewBufferedLog(8)

	assert.Equal(t, []byte{}, buf.Data())
	assert.Equal(t, uint32(0), buf.WritePos())
	assert.Equal(t, uint32(0), buf.ReadPos())
}

func TestLogBuffer_TryWrite_One(t *testing.T) {
	buf := NewBufferedLog(4)
	assert.Equal(t, uint32(1), buf.TryWrite([]byte{1}))
	assert.Equal(t, []byte{1}, buf.Data())
	assert.Equal(t, uint32(1), buf.WritePos())
	assert.Equal(t, uint32(0), buf.ReadPos())
}

func TestLogBuffer_TryWrite_ToCapacity(t *testing.T) {
	buf := NewBufferedLog(4)

	assert.Equal(t, uint32(4), buf.TryWrite([]byte{1, 2, 3, 4}))
	assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())
	assert.Equal(t, uint32(4), buf.WritePos())
	assert.Equal(t, uint32(0), buf.ReadPos())
}

func TestLogBuffer_TryWrite_BeyondCapacity(t *testing.T) {
	buf := NewBufferedLog(4)

	assert.Equal(t, uint32(4), buf.TryWrite([]byte{1, 2, 3, 4}))
	assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())
	assert.Equal(t, uint32(4), buf.WritePos())
	assert.Equal(t, uint32(0), buf.ReadPos())

	assert.Equal(t, uint32(0), buf.TryWrite([]byte{4, 5, 6}))
	assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())
	assert.Equal(t, uint32(4), buf.WritePos())
	assert.Equal(t, uint32(0), buf.ReadPos())
}

func TestLogBuffer_Write_LessThanCapacity(t *testing.T) {
	buf := NewBufferedLog(4)

	num, err := buf.Write([]byte{1})
	assert.Equal(t, 1, num)
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte{1}, buf.Data())
	assert.Equal(t, uint32(1), buf.WritePos())
	assert.Equal(t, uint32(0), buf.ReadPos())
}

func TestLogBuffer_Write_EqualToCapacity(t *testing.T) {
	buf := NewBufferedLog(4)

	num, err := buf.Write([]byte{1, 2, 3, 4})
	assert.Equal(t, 4, num)
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())
	assert.Equal(t, uint32(4), buf.WritePos())
	assert.Equal(t, uint32(0), buf.ReadPos())
}

func TestLogBuffer_Write_GreaterCapacity(t *testing.T) {
	buf := NewBufferedLog(4)
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

func TestActiveChannel_simple(t *testing.T) {

}








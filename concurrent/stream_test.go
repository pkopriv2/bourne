package concurrent

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStream_init(t *testing.T) {
	buf := NewStream(8)

	assert.Equal(t, []byte{}, buf.Data())
	tail, cur, head, _ := buf.Snapshot()

	assert.Equal(t, uint64(0), tail.offset)
	assert.Equal(t, uint64(0), cur.offset)
	assert.Equal(t, uint64(0), head.offset)
}

func TestStream_TryWrite_One(t *testing.T) {
	buf := NewStream(4)

	num, _, _ := buf.TryWrite([]byte{1})

	assert.Equal(t, uint64(1), num)
	assert.Equal(t, []byte{1}, buf.Data())

	tail, cur, head, _ := buf.Snapshot()

	assert.Equal(t, uint64(0), tail.offset)
	assert.Equal(t, uint64(0), cur.offset)
	assert.Equal(t, uint64(1), head.offset)
}


func TestStream_TryWrite_ToCapacity(t *testing.T) {
	buf := NewStream(4)

	num, _, _ := buf.TryWrite([]byte{1,2,3,4})
	assert.Equal(t, uint64(4), num)
	assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())

	tail, cur, head, _ := buf.Snapshot()
	assert.Equal(t, uint64(0), tail.offset)
	assert.Equal(t, uint64(0), cur.offset)
	assert.Equal(t, uint64(4), head.offset)
}

func TestStream_TryWrite_BeyondCapacity(t *testing.T) {
	buf := NewStream(4)

	num, _, _ := buf.TryWrite([]byte{1,2,3,4})
	assert.Equal(t, uint64(4), num)
	assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())

	num, _, _ = buf.TryWrite([]byte{1,2,3,4})
	assert.Equal(t, uint64(0), num)
	assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())

	tail, cur, head, _ := buf.Snapshot()
	assert.Equal(t, uint64(0), tail.offset)
	assert.Equal(t, uint64(0), cur.offset)
	assert.Equal(t, uint64(4), head.offset)
}

func TestStream_TryRead_Empty(t *testing.T) {
	buf := NewStream(4)

	start, num, _ := buf.TryRead([]byte{0}, false)
	assert.Equal(t, uint64(0), start.offset)
	assert.Equal(t, uint64(0), num)

	tail, cur, head, _ := buf.Snapshot()
	assert.Equal(t, uint64(0), tail.offset)
	assert.Equal(t, uint64(0), cur.offset)
	assert.Equal(t, uint64(0), head.offset)
}

func TestStream_TryRead_One(t *testing.T) {
	buf := NewStream(4)

	buf.TryWrite([]byte{1,2,3,4})

	out := make([]byte, 1)
	start, num, _ := buf.TryRead(out, false)
	assert.Equal(t, uint64(0), start.offset)
	assert.Equal(t, uint64(1), num)
	assert.Equal(t, []byte{1}, out)

	tail, cur, head, _ := buf.Snapshot()
	assert.Equal(t, uint64(0), tail.offset)
	assert.Equal(t, uint64(1), cur.offset)
	assert.Equal(t, uint64(4), head.offset)
}

func TestStream_Wrap(t *testing.T) {
	buf := NewStream(4)

	buf.TryWrite([]byte{1,2,3,4})

	out := make([]byte, 2)
	start, num, _ := buf.TryRead(out, true)
	assert.Equal(t, uint64(0), start.offset)
	assert.Equal(t, uint64(2), num)
	assert.Equal(t, []byte{1,2}, out)

	// the next write should wrap
	buf.TryWrite([]byte{5,6})

	tail, cur, head, _ := buf.Snapshot()
	assert.Equal(t, uint64(2), tail.offset)
	assert.Equal(t, uint64(2), cur.offset)
	assert.Equal(t, uint64(6), head.offset)

	assert.Equal(t, []byte{3,4,5,6}, buf.Data())
}

package wire

import (
	// "fmt"
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	uuid "github.com/satori/go.uuid"
)

func TestParcelSerialization_Empty(t *testing.T) {
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)
	reader := bufio.NewReader(buf)

	orig := NewParcel()
	orig.Write(writer)

	read, _ := ReadParcel(reader)

	assert.Equal(t, orig, read)
}

func TestParcelSerialization_Uint8(t *testing.T) {
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)
	reader := bufio.NewReader(buf)

	orig := NewParcel()
	orig.Set(NewMessageId(1), uint8(1))
	orig.Write(writer)

	read, _ := ReadParcel(reader)

	assert.Equal(t, orig, read)
}

func TestParcelSerialization_Uint16(t *testing.T) {
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)
	reader := bufio.NewReader(buf)

	orig := NewParcel()
	orig.Set(NewMessageId(1), uint16(1))
	orig.Write(writer)

	read, _ := ReadParcel(reader)

	assert.Equal(t, orig, read)
}

func TestParcelSerialization_Uint32(t *testing.T) {
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)
	reader := bufio.NewReader(buf)

	orig := NewParcel()
	orig.Set(NewMessageId(1), uint32(1))
	orig.Write(writer)

	read, _ := ReadParcel(reader)

	assert.Equal(t, orig, read)
}

func TestParcelSerialization_Uint64(t *testing.T) {
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)
	reader := bufio.NewReader(buf)

	orig := NewParcel()
	orig.Set(NewMessageId(1), uint64(1))
	orig.Write(writer)

	read, _ := ReadParcel(reader)

	assert.Equal(t, orig, read)
}

func TestParcelSerialization_String(t *testing.T) {
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)
	reader := bufio.NewReader(buf)

	orig := NewParcel()
	orig.Set(NewMessageId(1), "string")
	orig.Write(writer)

	read, _ := ReadParcel(reader)

	assert.Equal(t, orig, read)
}

func TestParcelSerialization_all(t *testing.T) {
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)
	reader := bufio.NewReader(buf)

	id1 := NewMessageId(1)
	id2 := NewMessageId(1 << 1)
	id3 := NewMessageId(1 << 2)
	id4 := NewMessageId(1 << 3)
	id5 := NewMessageId(1 << 4)
	id6 := NewMessageId(1 << 5)
	id7 := NewMessageId(1 << 6)

	orig := NewParcel()
	orig.Set(id1, uint8(1))
	orig.Set(id2, uint16(2))
	orig.Set(id3, uint32(3))
	orig.Set(id4, uint64(4))
	orig.Set(id5, "string")
	orig.Set(id6, uuid.NewV4())
	orig.Set(id7, []byte{1, 2, 3, 4})

	orig.Write(writer)

	read, _ := ReadParcel(reader)

	assert.Equal(t, orig, read)
}

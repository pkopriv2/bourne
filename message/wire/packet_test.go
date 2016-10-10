package wire

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestPacket_Empty(t *testing.T) {
	src := NewAddress(uuid.NewV4(), 0)
	dst := NewAddress(uuid.NewV4(), 1)

	p := BuildPacket(NewRemoteRoute(src, dst)).Build()
	assert.Equal(t, nil, p.Open())
}

func TestPacketSerialization_Open(t *testing.T) {
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)
	reader := bufio.NewReader(buf)

	src := NewAddress(uuid.NewV4(), 0)
	dst := NewAddress(uuid.NewV4(), 1)

	out := BuildPacket(NewRemoteRoute(src, dst)).SetOpen(1).Build()
	out.Write(writer)

	in, err := ReadPacket(reader)
	fmt.Println(out)
	fmt.Println(in)

	assert.Equal(t, nil, err)
	assert.Equal(t, out, in)
}

func TestPacketSerialization_Multi(t *testing.T) {
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)
	reader := bufio.NewReader(buf)

	src := NewAddress(uuid.NewV4(), 0)
	dst := NewAddress(uuid.NewV4(), 1)

	builder := BuildPacket(NewRemoteRoute(src, dst))
	builder.SetOpen(1)
	builder.SetClose(2)
	builder.SetVerify(3)
	builder.SetError(&ProtocolError{1, "error"})
	builder.SetSegment(4, []byte{1, 2, 3, 4})

	packet := builder.Build()
	packet.Write(writer)

	read, err := ReadPacket(reader)
	fmt.Println(read)

	assert.Equal(t, nil, err)
	assert.Equal(t, packet, read)
}

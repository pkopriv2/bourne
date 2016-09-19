package wire

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestPacketSerialization_Empty(t *testing.T) {
	src := NewEndPoint(uuid.NewV4(), 0)
	dst := NewEndPoint(uuid.NewV4(), 1)

	packet := NewPacket(NewRemoteRoute(src, dst))

	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)
	reader := bufio.NewReader(buf)

	WritePacket(writer, packet)
	// fmt.Println(len(buf.Bytes()))

	read, err := ReadPacket(reader)
	fmt.Println(read)

	assert.Equal(t, nil, err)
	assert.Equal(t, packet, read)
}

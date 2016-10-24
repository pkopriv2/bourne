package conn

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemConnection_simple(t *testing.T) {
	conn := NewMemConnection()
	go func() {
		for i := 0; i < 1024; i++ {
			conn.Write([]byte{byte(i)})
		}
	}()

	buf := make([]byte, 1024)
	io.ReadFull(conn, buf)

	for i := 0; i < 1024; i++ {
		assert.Equal(t, buf[i], byte(i))
	}
}

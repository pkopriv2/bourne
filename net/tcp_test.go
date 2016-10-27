package net

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TODO: randomize port assignment.
func TestTCPListener_Close(t *testing.T) {
	listener, _ := ListenTCP(0)
	assert.Nil(t, listener.Close())
}

func TestTCPListener_Accept(t *testing.T) {
	listener, _ := ListenTCP(0)
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		assert.NotNil(t, conn)
		assert.Nil(t, err)
		conn.Close()
	}()

	conn, err := listener.Conn()
	assert.NotNil(t, conn)
	assert.Nil(t, err)
	conn.Close()
}

func TestTCPListener_Read_Write(t *testing.T) {
	listener, _ := ListenTCP(9000)
	defer listener.Close()

	go func() {
		conn, _ := listener.Accept()
		defer conn.Close()
		for i := 0; i < 1024; i++ {
			if _, err := conn.Write([]byte{byte(i)}); err != nil {
				t.Fail()
			}
		}
	}()

	buf := make([]byte, 1024)

	conn, _ := listener.Conn()
	defer conn.Close()
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.FailNow()
	}

	for i := 0; i < 1024; i++ {
		assert.Equal(t, byte(i), buf[i])
	}
}

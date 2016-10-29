package net

import (
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

// func TestTCPConnectionFactory_Serialize(t *testing.T) {
	// factory1 := NewTCPConnectionFactory("localhost:9000")
	// // factory2, err := ParseConnectionFactory(factory1.Serialize())
	// // assert.Nil(t, err)
	// // assert.Equal(t, factory1, factory2)
// }

func TestTCPConnectionFactory_Serialize_JSON(t *testing.T) {
	factory1 := NewTCPConnectionFactory("localhost:9000")
	bytes, err := json.Marshal(factory1.Serialize())

	var ret interface{}
	err = json.Unmarshal(bytes, &ret)

	assert.Nil(t, err)
	assert.Equal(t, "hello", ret)
}

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

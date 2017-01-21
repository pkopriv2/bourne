package net

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConnectionPool_Close(t *testing.T) {
	pool := NewConnectionPool("tcp", "localhost:0", 10, 5*time.Second)
	assert.Nil(t, pool.Close())
}

func TestConnectionPool_TakeTimeout_Timeout(t *testing.T) {
	pool := NewConnectionPool("tcp", "localhost:0", 10, 5*time.Second)
	defer pool.Close()
	assert.Nil(t, pool.TakeTimeout(100*time.Millisecond))
}

func TestConnectionPool_TakeTimeout_Success(t *testing.T) {
	listener, _ := ListenTcp("0")
	defer listener.Close()
	pool := NewConnectionPool("tcp", listener.Addr().String(), 10, 5*time.Second)
	defer pool.Close()
	assert.NotNil(t, pool.TakeTimeout(100*time.Millisecond))
}

func TestConnectionPool_TakeTimeout_Fail_Recover(t *testing.T) {
	listener, _ := ListenTcp("0")
	listener.Close()

	pool := NewConnectionPool("tcp", listener.Addr().String(), 10, 100*time.Millisecond)
	defer pool.Close()
	assert.Nil(t, pool.TakeTimeout(100*time.Millisecond))

	_, port, e := SplitAddr(listener.Addr().String())
	assert.Nil(t, e)

	time.Sleep(1 * time.Second)
	listener, e = ListenTcp(port)

	assert.Nil(t, e)
	go func() {
		conn, err := listener.Accept()
		assert.Nil(t, err)
		defer conn.Close()
	}()

	defer listener.Close()
	assert.NotNil(t, pool.TakeTimeout(100*time.Millisecond))
}

func TestConnectionPool_TakeTimeout_Succeed_Fail_Recover(t *testing.T) {
	listener, _ := ListenTcp("0")
	go func() {
		conn, err := listener.Accept()
		assert.Nil(t, err)
		conn.Close()
	}()

	pool := NewConnectionPool("tcp", listener.Addr().String(), 10, 100*time.Millisecond)
	defer pool.Close()
	assert.NotNil(t, pool.TakeTimeout(100*time.Millisecond))

	listener.Close()
	assert.Nil(t, pool.TakeTimeout(100*time.Millisecond))

	_, port, e := SplitAddr(listener.Addr().String())
	assert.Nil(t, e)

	listener, _ = ListenTcp(port)
	go func() {
		conn, err := listener.Accept()
		assert.Nil(t, err)
		conn.Close()
	}()

	defer listener.Close()
	assert.NotNil(t, pool.TakeTimeout(100*time.Millisecond))
}

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
	assert.Nil(t, pool.TakeTimeout(1000*time.Millisecond))
}

func TestConnectionPool_TakeTimeout_Success(t *testing.T) {
	listener, _ := ListenTcp("0")
	defer listener.Close()
	pool := NewConnectionPool("tcp", listener.Addr().String(), 10, 5*time.Second)
	defer pool.Close()
	assert.NotNil(t, pool.TakeTimeout(1000*time.Millisecond))
}

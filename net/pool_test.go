package net

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConnectionPool_Close(t *testing.T) {
	pool := NewConnectionPool(&TcpNetwork{}, "localhost:0", 10, 5*time.Second)
	assert.Nil(t, pool.Close())
}

func TestConnectionPool_TakeTimeout_Timeout(t *testing.T) {
	pool := NewConnectionPool(&TcpNetwork{}, "localhost:0", 10, 5*time.Second)
	defer pool.Close()
	assert.Nil(t, pool.TakeTimeout(100*time.Millisecond))
}

func TestConnectionPool_TakeTimeout_Success(t *testing.T) {
	listener, _ := ListenTcp(10*time.Second, "localhost:0")
	defer listener.Close()
	pool := NewConnectionPool(NewTcpNetwork(), listener.Addr().String(), 10, 5*time.Second)
	defer pool.Close()
	assert.NotNil(t, pool.TakeTimeout(100*time.Millisecond))
}

// func TestConnectionPool_TakeTimeout_Fail_Recover(t *testing.T) {
// listener, _ := ListenTcp(10*time.Second, ":0")
// listener.Close()
//
// pool := NewConnectionPool(NewTcpNetwork(), listener.Addr().String(), 10, 100*time.Millisecond)
// assert.Nil(t, pool.TakeTimeout(100*time.Millisecond))
// defer pool.Close()
//
// time.Sleep(1 * time.Second)
//
// listener, e := ListenTcp(10*time.Second, listener.Addr().String())
// assert.Nil(t, e)
//
// time.Sleep(1 * time.Second)
//
// defer listener.Close()
// go func() {
// conn, err := listener.Accept()
// assert.Nil(t, err)
// defer conn.Close()
// }()
//
// assert.NotNil(t, pool.TakeTimeout(100*time.Millisecond))
// }
//
// func TestConnectionPool_TakeTimeout_Succeed_Fail_Recover(t *testing.T) {
// listener, _ := ListenTcp(10*time.Second, ":0")
// go func() {
// conn, err := listener.Accept()
// if err == nil {
// defer conn.Close()
// }
// assert.Nil(t, err)
// }()
// fmt.Println(listener.Addr().String())
//
// pool := NewConnectionPool(NewTcpNetwork(), listener.Addr().String(), 10, 100*time.Millisecond)
// defer pool.Close()
//
// assert.NotNil(t, pool.TakeTimeout(1000*time.Millisecond))
// listener.Close()
// assert.Nil(t, pool.TakeTimeout(1000*time.Millisecond))
//
// time.Sleep(500*time.Millisecond)
//
// listener, err := ListenTcp(10*time.Second, listener.Addr().String())
// assert.Nil(t, err)
//
// go func() {
// conn, err := listener.Accept()
// if err == nil {
// defer conn.Close()
// }
// assert.Nil(t, err)
// }()
//
// defer listener.Close()
// time.Sleep(500*time.Millisecond)
// assert.NotNil(t, pool.TakeTimeout(5000*time.Millisecond))
// time.Sleep(100*time.Millisecond)
// }

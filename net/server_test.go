package net

import (
	"errors"
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/stretchr/testify/assert"
)

var failure = errors.New("failure")

var successHandler = func(Request) Response {
	return NewSuccessResponse(nil)
}

var failureHandler = func(Request) Response {
	return NewErrorResponse(failure)
}

func NewTestServer(fn Handler, opts ...ServerOptionsFn) Server {
	server, err := NewTcpServer(common.NewContext(common.NewEmptyConfig()), 0, fn, opts...)
	if err != nil {
		panic(err)
	}

	return server
}

func TestServer_Close(t *testing.T) {
	server := NewTestServer(successHandler)
	assert.Nil(t, server.Close())
}

func TestClientServer_SingleRequest(t *testing.T) {
	called := concurrent.NewAtomicBool()
	server := NewTestServer(func(r Request) Response {
		called.Set(true)
		assert.Equal(t, 0, r.Type())
		assert.Nil(t, r.Body())
		return NewSuccessResponse(nil)
	})
	defer server.Close()

	client, _ := server.Client()
	defer client.Close()

	resp, err := client.Send(NewRequest(0, nil))
	assert.True(t, called.Get())
	assert.Nil(t, err)
	assert.Nil(t, resp.Error())
}

func TestClientServer_MultiRequest(t *testing.T) {
	called := concurrent.NewAtomicCounter()
	server := NewTestServer(func(r Request) Response {
		called.Inc()
		assert.Equal(t, 0, r.Type())
		assert.Nil(t, r.Body())
		return NewSuccessResponse(nil)
	})
	defer server.Close()

	client, _ := server.Client()
	defer client.Close()

	for i := 0; i < 10; i++ {
		resp, err := client.Send(NewRequest(0, nil))
		assert.Nil(t, err)
		assert.NotNil(t, resp)
		assert.Nil(t, resp.Error())
	}

	assert.Equal(t, uint64(10), called.Get())
}

func TestClientServer_RecvTimeout(t *testing.T) {
	server := NewTestServer(successHandler, func(opts *ServerOptions) {
		opts.RecvTimeout = 100 * time.Millisecond
	})
	defer server.Close()

	client, _ := server.Client()
	defer client.Close()

	time.Sleep(500 * time.Millisecond)
	_, err := client.Send(NewRequest(0, nil))
	assert.NotNil(t, err)
}

func TestClientServer_ServerClosed(t *testing.T) {
	server := NewTestServer(func(Request) Response {
		time.Sleep(5 * time.Second)
		return NewEmptyResponse()
	})
	defer server.Close()

	client, _ := server.Client()
	defer client.Close()

	barrier1 := make(chan struct{}, 1)
	barrier2 := make(chan struct{}, 1)
	go func() {
		barrier1 <- struct{}{}
		_, err := client.Send(NewRequest(0, nil))
		assert.NotNil(t, err)
		barrier2 <- struct{}{}
	}()

	<-barrier1
	assert.Nil(t, server.Close())
	<-barrier2

}

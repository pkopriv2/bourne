package net

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var failure = errors.New("failure")

var successHandler = func(Request) Response {
	return NewSuccessResponse(nil)
}

var failureHandler = func(Request) Response {
	return NewErrorResponse(failure)
}

func TestServer_Close(t *testing.T) {
	server, _ := NewTCPServer(0, successHandler)
	assert.Nil(t, server.Close())
}

func TestClientServer_SingleRequest(t *testing.T) {
	server, _ := NewTCPServer(0, successHandler)
	defer server.Close()

	client, _ := server.Client()
	defer client.Close()

	resp, err := client.Send(NewRequest(0, nil))
	assert.Nil(t, err)
	assert.True(t, resp.Success)
}

func TestClientServer_MultiRequest(t *testing.T) {
	server, _ := NewTCPServer(0, successHandler)
	defer server.Close()

	client, _ := server.Client()
	defer client.Close()

	for i := 0; i < 10; i++ {
		resp, err := client.Send(NewRequest(0, nil))
		assert.Nil(t, err)
		assert.True(t, resp.Success)
	}
}

func TestClientServer_RecvTimeout(t *testing.T) {
	server, err := NewTCPServer(0, successHandler, func(opts *ServerOptions) {
		opts.RecvTimeout = 100 * time.Millisecond
	})
	defer server.Close()

	client, _ := server.Client()
	defer client.Close()

	for i := 0; i < 10; i++ {
		resp, err := client.Send(NewRequest(0, nil))
		assert.Nil(t, err)
		assert.True(t, resp.Success)
	}

	time.Sleep(500 * time.Millisecond)
	_, err = client.Send(NewRequest(0, nil))
	assert.NotNil(t, err)
}

func TestClientServer_ServerClosed(t *testing.T) {
	server, err := NewTCPServer(0, func(Request) Response {
		time.Sleep(5 * time.Second)
		return NewSuccessResponse(nil)
	})

	client, _ := server.Client()
	defer client.Close()

	barrier1 := make(chan struct{}, 1)
	barrier2 := make(chan struct{}, 1)
	go func() {
		barrier1 <- struct{}{}
		_, err = client.Send(NewRequest(0, nil))
		assert.NotNil(t, err)
		barrier2 <- struct{}{}
	}()

	<-barrier1
	assert.Nil(t, server.Close())
	<-barrier2

}

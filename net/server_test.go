package net

import (
	"errors"
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/enc"
	"github.com/stretchr/testify/assert"
)

var failure = errors.New("failure")

var successHandler = func(Request) Response {
	return NewStandardResponse(nil)
}

var failureHandler = func(Request) Response {
	return NewErrorResponse(failure)
}

func NewTestServer(fn Handler, config common.Config) Server {
	if config == nil {
		config = common.NewEmptyConfig()
	}

	server, err := NewTcpServer(common.NewContext(config), 0, fn)
	if err != nil {
		panic(err)
	}

	return server
}

func TestServer_Close(t *testing.T) {
	server := NewTestServer(successHandler, nil)
	assert.Nil(t, server.Close())
}

func TestClientServer_SingleRequest(t *testing.T) {
	msg, err := enc.BuildMessage(func(w enc.Writer) {
		w.Write("field1", 1)
		w.Write("field2", int8(1))
		w.Write("field3", true)
	})

	called := concurrent.NewAtomicBool()
	server := NewTestServer(func(r Request) Response {
		called.Set(true)
		assert.Equal(t, 0, r.Type())

		var field1 int
		var field2 int8
		var field3 bool
		assert.Nil(t, r.Body().Read("field1", &field1))
		assert.Nil(t, r.Body().Read("field2", &field2))
		assert.Nil(t, r.Body().Read("field3", &field3))
		assert.Equal(t, 1, field1)
		assert.Equal(t, int8(1), field2)
		assert.Equal(t, true, field3)

		return NewStandardResponse(msg)
	}, nil)
	defer server.Close()

	client, _ := server.Client()
	defer client.Close()

	resp, err := client.Send(NewRequest(0, msg))
	assert.True(t, called.Get())
	assert.Nil(t, err)
	assert.Nil(t, resp.Error())

	var field1 int
	var field2 int8
	var field3 bool
	assert.Nil(t, resp.Body().Read("field1", &field1))
	assert.Nil(t, resp.Body().Read("field2", &field2))
	assert.Nil(t, resp.Body().Read("field3", &field3))
	assert.Equal(t, 1, field1)
	assert.Equal(t, int8(1), field2)
	assert.Equal(t, true, field3)
}

func TestClientServer_MultiRequest(t *testing.T) {
	called := concurrent.NewAtomicCounter()
	server := NewTestServer(func(r Request) Response {
		called.Inc()
		assert.Equal(t, 0, r.Type())
		assert.Nil(t, r.Body())
		return NewStandardResponse(nil)
	}, nil)
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
	config := common.NewConfig(map[string]interface{}{
		ConfServerRecvTimeout: 100})

	server := NewTestServer(successHandler, config)
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
	}, nil)
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

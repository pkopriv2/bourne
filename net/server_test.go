package net

import (
	"errors"
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/scribe"
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
	ctx := common.NewContext(config)

	server, err := NewTcpServer(ctx, ctx.Logger(), "0", fn)
	if err != nil {
		panic(err)
	}

	return server
}

func TestServer_Close(t *testing.T) {
	server := NewTestServer(successHandler, nil)
	assert.Nil(t, server.Close())
}

func TestClient_Close(t *testing.T) {
	server := NewTestServer(successHandler, nil)
	defer server.Close()

	client, err := server.Client()
	assert.Nil(t, err)
	assert.Nil(t, client.Close())

	_, err = client.Send(NewStandardRequest(nil))
	assert.NotNil(t, err)
}

func TestClient_ServerCloseBefore(t *testing.T) {
	server := NewTestServer(successHandler, nil)
	assert.Nil(t, server.Close())

	_, err := server.Client()
	assert.NotNil(t, err)
}

func TestClient_ServerCloseAfter(t *testing.T) {
	server := NewTestServer(successHandler, nil)

	client, err := server.Client()
	assert.Nil(t, err)
	assert.Nil(t, server.Close())

	_, err = client.Send(NewStandardRequest(nil))
	assert.NotNil(t, err)
}

func TestClientServer_SingleRequest(t *testing.T) {
	msg := scribe.Build(func(w scribe.Writer) {
		w.WriteInt("field1", 1)
		w.WriteBool("field2", true)
	})

	called := concurrent.NewAtomicBool()
	server := NewTestServer(func(r Request) Response {
		called.Set(true)

		var field1 int
		var field2 bool
		assert.Nil(t, r.Body().ReadInt("field1", &field1))
		assert.Nil(t, r.Body().ReadBool("field2", &field2))
		assert.Equal(t, 1, field1)
		assert.Equal(t, true, field2)

		return NewStandardResponse(msg)
	}, nil)
	defer server.Close()

	client, _ := server.Client()
	defer client.Close()

	resp, err := client.Send(NewStandardRequest(msg))
	assert.True(t, called.Get())
	assert.Nil(t, err)
	assert.Nil(t, resp.Error())

	var field1 int
	var field2 bool
	assert.Nil(t, resp.Body().ReadInt("field1", &field1))
	assert.Nil(t, resp.Body().ReadBool("field2", &field2))
	assert.Equal(t, 1, field1)
	assert.Equal(t, true, field2)
}

func TestClientServer_MultiRequest(t *testing.T) {
	called := concurrent.NewAtomicCounter()
	server := NewTestServer(func(r Request) Response {
		called.Inc()
		assert.Equal(t, scribe.EmptyMessage, r.Body())
		return NewStandardResponse(nil)
	}, nil)
	defer server.Close()

	client, _ := server.Client()
	defer client.Close()

	for i := 0; i < 10; i++ {
		resp, err := client.Send(NewStandardRequest(nil))
		assert.Nil(t, err)
		assert.NotNil(t, resp)
		assert.Nil(t, resp.Error())
	}

	assert.Equal(t, uint64(10), called.Get())
}

// func TestClientServer_RecvTimeout(t *testing.T) {
	// config := common.NewConfig(map[string]interface{}{
		// ConfServerRecvTimeout: 100})
//
	// server := NewTestServer(successHandler, config)
	// server.Close()
//
	// client, _ := server.Client()
	// defer client.Close()
//
	// time.Sleep(500 * time.Millisecond)
//
	// _, err := client.Send(NewStandardRequest(nil))
	// assert.NotNil(t, err)
// }

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
		_, err := client.Send(NewStandardRequest(nil))
		assert.NotNil(t, err)
		barrier2 <- struct{}{}
	}()

	<-barrier1
	assert.Nil(t, server.Close())
	<-barrier2

}

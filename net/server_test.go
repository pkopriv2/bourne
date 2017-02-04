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

func NewTestServer(ctx common.Context, fn Handler) Server {
	network := &TcpNetwork{}
	l, err := network.Listen(10*time.Second, ":0")
	if err != nil {
		panic(err)
	}

	server, err := NewServer(ctx, l, fn, 10)
	if err != nil {
		panic(err)
	}

	return server
}

func TestServer_Close(t *testing.T) {
	ctx := common.NewEmptyContext()
	defer ctx.Close()
	server := NewTestServer(ctx, successHandler)
	assert.Nil(t, server.Close())
}

func TestClient_Close(t *testing.T) {
	ctx := common.NewEmptyContext()
	defer ctx.Close()
	server := NewTestServer(ctx, successHandler)

	client, err := server.Client(Json)
	assert.Nil(t, err)
	assert.Nil(t, client.Close())

	_, err = client.Send(NewStandardRequest(nil))
	assert.NotNil(t, err)
}

func TestClient_ServerCloseBefore(t *testing.T) {
	ctx := common.NewEmptyContext()
	defer ctx.Close()
	server := NewTestServer(ctx, successHandler)
	server.Close()

	_, err := server.Client(Json)
	assert.NotNil(t, err)
}

func TestClient_ServerCloseAfter(t *testing.T) {
	ctx := common.NewEmptyContext()
	defer ctx.Close()
	server := NewTestServer(ctx, successHandler)

	client, err := server.Client(Json)
	assert.Nil(t, err)
	assert.Nil(t, server.Close())

	_, err = client.Send(NewStandardRequest(nil))
	assert.NotNil(t, err)
}

func TestClientServer_SingleRequest(t *testing.T) {
	ctx := common.NewEmptyContext()
	defer ctx.Close()

	msg := scribe.Build(func(w scribe.Writer) {
		w.WriteInt("field1", 1)
		w.WriteBool("field2", true)
	})

	called := concurrent.NewAtomicBool()
	server := NewTestServer(ctx, func(r Request) Response {
		called.Set(true)

		var field1 int
		var field2 bool
		assert.Nil(t, r.Body().ReadInt("field1", &field1))
		assert.Nil(t, r.Body().ReadBool("field2", &field2))
		assert.Equal(t, 1, field1)
		assert.Equal(t, true, field2)

		return NewStandardResponse(msg)
	})
	defer server.Close()

	client, _ := server.Client(Json)
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
	ctx := common.NewEmptyContext()
	defer ctx.Close()

	called := concurrent.NewAtomicCounter()
	server := NewTestServer(ctx, func(r Request) Response {
		called.Inc()
		assert.Equal(t, scribe.EmptyMessage, r.Body())
		return NewStandardResponse(nil)
	})
	defer server.Close()

	client, _ := server.Client(Json)
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
	ctx := common.NewEmptyContext()
	defer ctx.Close()

	server := NewTestServer(ctx, func(Request) Response {
		time.Sleep(5 * time.Second)
		return NewEmptyResponse()
	})
	defer server.Close()

	client, _ := server.Client(Json)
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

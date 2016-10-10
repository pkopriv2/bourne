package core

import (
	"errors"
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/message/wire"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func NoRoute(wire.Packet) interface{} {
	return nil
}

func TestMux_Close_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())

	mux := NewMultiplexer(ctx, NoRoute)
	mux.Close()
}

func TestMux_Close_SingleSocket(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())

	mux := NewMultiplexer(ctx, NoRoute)

	sock, _ := mux.AddSocket("1")
	go sock.Done()

	mux.Close()
}

func TestMux_Close_MultiSocket(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())

	mux := NewMultiplexer(ctx, NoRoute)

	sock1, _ := mux.AddSocket("1")
	sock2, _ := mux.AddSocket("2")
	sock3, _ := mux.AddSocket("3")
	defer sock1.Done()
	defer sock2.Done()
	defer sock3.Done()

	go mux.Close()

	assert.NotNil(t, sock1.Closed())
	assert.NotNil(t, sock1.Closed())
	assert.NotNil(t, sock1.Closed())
}

func TestMux_Fail_Empty(t *testing.T) {
	err := errors.New("Error")
	mux := NewMultiplexer(common.NewContext(common.NewEmptyConfig()), NoRoute)
	mux.Fail(err)
}

func TestMux_Fail_NotDone(t *testing.T) {
	mux := NewMultiplexer(common.NewContext(common.NewEmptyConfig()), NoRoute)
	sock, _ := mux.AddSocket("1")
	defer sock.Done()

	assert.Nil(t, sock.Failure())
}

func TestMux_Fail_SingleSocket(t *testing.T) {
	mux := NewMultiplexer(common.NewContext(common.NewEmptyConfig()), NoRoute)
	sock, _ := mux.AddSocket("1")
	defer sock.Done()

	err := errors.New("Error")
	go mux.Fail(err)

	assert.NotNil(t, <-sock.Failed())
	assert.Equal(t, err, sock.Failure())
}

func TestMux_Fail_MultiSocket(t *testing.T) {
	mux := NewMultiplexer(common.NewContext(common.NewEmptyConfig()), NoRoute)
	sock1, _ := mux.AddSocket("1")
	sock2, _ := mux.AddSocket("2")
	sock3, _ := mux.AddSocket("3")
	defer sock1.Done()
	defer sock2.Done()
	defer sock3.Done()

	err := errors.New("Error")
	go mux.Fail(err)

	assert.NotNil(t, <-sock1.Failed())
	assert.NotNil(t, <-sock2.Failed())
	assert.NotNil(t, <-sock3.Failed())
	assert.Equal(t, err, sock1.Failure())
	assert.Equal(t, err, sock2.Failure())
	assert.Equal(t, err, sock3.Failure())
}

func TestMux_Route_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())

	src := wire.NewAddress(uuid.NewV4(), 0)
	dst := wire.NewAddress(uuid.NewV4(), 1)

	p := wire.BuildPacket(wire.NewRemoteRoute(src, dst)).Build()

	mux := NewMultiplexer(ctx, NoRoute)
	mux.Tx() <- p

	assert.Equal(t, p, <-mux.Return())
}

func TestMux_Route_NoExist(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	src := wire.NewAddress(uuid.NewV4(), 0)
	dst := wire.NewAddress(uuid.NewV4(), 1)
	p := wire.BuildPacket(wire.NewRemoteRoute(src, dst)).Build()

	mux := NewMultiplexer(ctx, func(wire.Packet) interface{} {
		return "noexist"
	})
	mux.Tx() <- p

	assert.Equal(t, p, <-mux.Return())
}

func TestMux_Route_Simple(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	src := wire.NewAddress(uuid.NewV4(), 0)
	dst := wire.NewAddress(uuid.NewV4(), 1)
	p := wire.BuildPacket(wire.NewRemoteRoute(src, dst)).Build()

	mux := NewMultiplexer(ctx, func(wire.Packet) interface{} {
		return "1"
	})

	sock, _ := mux.AddSocket("1")

	mux.Tx() <- p

	assert.Equal(t, p, <-sock.Rx())
}

func TestMux_Route_Multi(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	src := wire.NewAddress(uuid.NewV4(), 0)
	dst := wire.NewAddress(uuid.NewV4(), 1)
	p := wire.BuildPacket(wire.NewRemoteRoute(src, dst)).Build()

	mux := NewMultiplexer(ctx, func(wire.Packet) interface{} {
		return "1"
	})

	sock1, _ := mux.AddSocket("1")
	sock2, _ := mux.AddSocket("2")

	mux.Tx() <- p

	assert.Equal(t, p, <-sock1.Rx())
	select {
	default:
	case <-sock2.Rx():
		assert.Fail(t, "Erroneously received packet")
	}
}

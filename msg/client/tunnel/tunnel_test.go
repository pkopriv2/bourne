package tunnel

import (
	"io"
	"testing"

	"github.com/pkopriv2/bourne/msg/wire"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestTunnel_sendSinglePacket(t *testing.T) {
	tunnelL, tunnelR, cleanup := NewTestTunnelPair()
	defer tunnelR.Close()
	defer tunnelL.Close()
	defer cleanup()

	tunnelR.Write([]byte{1})

	buf := make([]byte, 1024)

	num, err := tunnelL.Read(buf)
	if err != nil {
		t.Fail()
	}
	assert.Equal(t, 1, num)
	assert.Equal(t, []byte{1}, buf[:1])
}

func TestTunnel_sendSingleStream(t *testing.T) {
	tunnelL, tunnelR, cleanup := NewTestTunnelPair()
	defer tunnelR.Close()
	defer tunnelL.Close()
	defer cleanup()

	go func() {
		for i := 0; i < 100; i++ {
			tunnelR.Write([]byte{byte(i)})
		}
	}()

	buf := make([]byte, 100)

	num, err := io.ReadFull(tunnelL, buf)
	assert.Nil(t, err)

	assert.Equal(t, 100, num)
	for i := 0; i < 100; i++ {
		assert.Equal(t, byte(i), buf[i])
	}
}

func TestTunnel_sendDuplexStream(t *testing.T) {
	tunnelL, tunnelR, cleanup := NewTestTunnelPair()
	defer tunnelR.Close()
	defer tunnelL.Close()
	defer cleanup()

	go func() {
		for i := 0; i < 100; i++ {
			tunnelR.Write([]byte{byte(i)})
		}
	}()

	go func() {
		for i := 0; i < 100; i++ {
			tunnelL.Write([]byte{byte(i)})
		}
	}()

	bufL := make([]byte, 100)
	bufR := make([]byte, 100)

	numL, errL := io.ReadFull(tunnelL, bufL)
	assert.Nil(t, errL)

	numR, errR := io.ReadFull(tunnelR, bufR)
	assert.Nil(t, errR)

	assert.Equal(t, 100, numL)
	assert.Equal(t, 100, numR)
	for i := 0; i < 100; i++ {
		assert.Equal(t, byte(i), bufL[i])
		assert.Equal(t, byte(i), bufR[i])
	}
}

func TestTunnel_sendSingleLargeStream(t *testing.T) {
	tunnelL, tunnelR, cleanup := NewTestTunnelPair()
	defer tunnelR.Close()
	defer tunnelL.Close()
	defer cleanup()

	buf := make([]byte, 1024)
	for i := 0; i < 1024; i++ {
		buf[i] = byte(i)
	}

	go func() {
		for i := 0; i < 1<<10; i++ {
			tunnelR.Write(buf)
		}
	}()

	actual := make([]byte, 1024)

	for i := 0; i < 1<<10 ; i++ {
		num, err := io.ReadFull(tunnelL, actual)
		assert.Nil(t, err)

		assert.Equal(t, 1024, num)
		for i := 0; i < 1024; i++ {
			assert.Equal(t, byte(i), actual[i])
		}
	}
}

func NewTestTunnel(memberIdL uuid.UUID, tunnelIdL uint64, memberIdR uuid.UUID, tunnelIdR uint64, listener bool) (*tunnel, chan wire.Packet) {
	l := wire.NewAddress(memberIdL, tunnelIdL)
	r := wire.NewAddress(memberIdR, tunnelIdR)

	mainSend := make(chan wire.Packet)
	t := NewTunnel(wire.NewRemoteRoute(l, r), mainSend, func(opts *TunnelOptions) {
		opts.Listening = listener
	})

	return t.(*tunnel), mainSend
}

func Split(in chan struct{}) (chan struct{}, chan struct{}) {
	one := make(chan struct{})
	two := make(chan struct{})
	go func() {
		for {
			i := <-in

			one <- i
			two <- i
		}
	}()

	return one, two
}

func StandardRoute(tunnelL *tunnel, mainSendL chan wire.Packet, tunnelR *tunnel, mainSendR chan wire.Packet) chan<- struct{} {
	// start left channel
	ret := make(chan struct{})

	lClose, rClose := Split(ret)
	route := func(tunnelOne *tunnel, mainSendOne chan wire.Packet, tunnelTwo *tunnel, close <-chan struct{}) {
		var l <-chan wire.Packet
		var r chan<- wire.Packet

		var p wire.Packet
		for {
			if p == nil {
				l = mainSendOne
				r = nil
			} else {
				l = nil
				r = tunnelTwo.channels.recvMain
			}

			select {
			case p = <-l:
			case r <- p:
				tunnelOne.env.logger.Debug("Routed packet: %v", p)
				p = nil
			case <-close:
				return
			}
		}
	}

	go route(tunnelL, mainSendL, tunnelR, lClose)
	go route(tunnelR, mainSendR, tunnelL, rClose)

	return ret
}

func NewTestTunnelPair() (*tunnel, *tunnel, func()) {
	lId := uuid.NewV4()
	rId := uuid.NewV4()
	tunnelL, mainSendL := NewTestTunnel(lId, 0, rId, 0, false)
	tunnelR, mainSendR := NewTestTunnel(rId, 0, lId, 0, true)

	close := StandardRoute(tunnelL, mainSendL, tunnelR, mainSendR)
	return tunnelL, tunnelR, (func() {
		close <- struct{}{}
	})
}

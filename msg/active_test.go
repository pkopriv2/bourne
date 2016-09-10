package msg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestChannelActive_openInitTimeout(t *testing.T) {
	_, channel := newTestChannel(0, 0, 1, 1, false)
	defer channel.Close()

	channel.state.WaitUntil(ChannelOpened | ChannelClosed)

	assert.Equal(t, ChannelClosed, channel.state.Get())
}

func TestChannelActive_openRecvTimeout(t *testing.T) {
	_, channel := newTestChannel(0, 0, 1, 1, true)
	defer channel.Close()

	// start the sequence, but never respond
	channel.send(newPacket(channel, PacketFlagOpen, 100, 0, []byte{}))
	channel.state.WaitUntil(ChannelOpened | ChannelClosed)

	assert.Equal(t, ChannelClosed, channel.state.Get())
}

func TestChannelActive_openHandshake(t *testing.T) {
	channelL, channelR := newTestChannelPair(0, 0, 1, 1)
	defer channelL.Close()
	defer channelR.Close()

	channelL.state.WaitUntil(ChannelOpened | ChannelClosed)
	channelR.state.WaitUntil(ChannelOpened | ChannelClosed)

	assert.Equal(t, ChannelOpened, channelL.state.Get())
	assert.Equal(t, ChannelOpened, channelR.state.Get())
}

func TestChannelActive_sendSinglePacket(t *testing.T) {
	channelL, channelR := newTestChannelPair(0, 0, 1, 1)
	defer channelR.Close()
	defer channelL.Close()

	channelR.Write([]byte{1})

	buf := make([]byte, 1024)

	num, err := channelL.Read(buf)
	if err != nil {
		t.Fail()
	}

	assert.Equal(t, 1, num)
	assert.Equal(t, []byte{1}, buf[:1])
}

func TestChannelActive_sendSingleStream(t *testing.T) {
	channelL, channelR := newTestChannelPair(0, 0, 1, 1)
	defer channelR.Close()
	defer channelL.Close()

	go func() {
		for i := 0; i < 255; i++ {
			channelL.Write([]byte{uint8(i)})
		}
	}()

	timer := time.NewTimer(time.Second * 10)

	buf := make([]byte, 1024)
	tot := 0
	for {
		select {
		case <-timer.C:
			t.Fail()
			return
		default:
			break
		}

		num, err := channelR.Read(buf[tot:])
		if err != nil {
			t.Fail()
			break
		}

		tot += num
		if tot >= 255 {
			break
		}
	}

	assert.Equal(t, 255, tot)
	for i := 0; i < 255; i++ {
		assert.Equal(t, uint8(i), buf[i])
	}
}

func TestChannelActive_sendDuplexStream(t *testing.T) {
	channelL, channelR := newTestChannelPair(0, 0, 1, 1)
	defer channelR.Close()
	defer channelL.Close()

	go func() {
		for i := 0; i < 255; i++ {
			channelL.Write([]byte{uint8(i)})
		}
	}()

	go func() {
		for i := 0; i < 255; i++ {
			channelR.Write([]byte{uint8(i)})
		}
	}()

	timer := time.NewTimer(time.Second * 10)

	bufR := make([]byte, 1024)
	totR := 0
	for {
		select {
		case <-timer.C:
			t.Fail()
			return
		default:
			break
		}

		num, err := channelR.Read(bufR[totR:])
		if err != nil {
			t.Fail()
			break
		}

		totR += num
		if totR >= 255 {
			break
		}
	}

	assert.Equal(t, 255, totR)
	for i := 0; i < 255; i++ {
		assert.Equal(t, uint8(i), bufR[i])
	}

	bufL := make([]byte, 1024)
	totL := 0
	for {
		select {
		case <-timer.C:
			t.Fail()
			return
		default:
			break
		}

		num, err := channelL.Read(bufL[totL:])
		if err != nil {
			t.Fail()
			break
		}

		totL += num
		if totL >= 255 {
			break
		}
	}

	assert.Equal(t, 255, totL)
	for i := 0; i < 255; i++ {
		assert.Equal(t, uint8(i), bufL[i])
	}
}

func TestChannelActive_sendLargeStream(t *testing.T) {
	channelL, channelR := newTestChannelPair(0, 0, 1, 1)
	defer channelR.Close()
	defer channelL.Close()

	go func() {
		for i := 0; i < 1<<20; i++ {
			channelL.Write([]byte{uint8(i)})
		}
	}()

	timer := time.NewTimer(time.Second * 30)

	buf := make([]byte, 1024)
	tot := 0
	for {
		select {
		case <-timer.C:
			t.Fail()
			return
		default:
			break
		}

		num, err := channelR.Read(buf)
		if err != nil {
			t.Fail()
			break
		}

		tot += num
		if tot >= 1<<20 {
			break
		}
	}

	assert.Equal(t, 1<<20, tot)
}

func newTestChannel(entityIdL uint32, channelIdL uint16, entityIdR uint32, channelIdR uint16, listener bool) (chan Packet, *ChannelActive) {
	l := ChannelAddress{entityIdL, channelIdL}
	r := ChannelAddress{entityIdR, channelIdR}

	out := make(chan Packet, 1024)

	channel, _ := NewChannelActive(l, r, listener, func(opts *ChannelOptions) {
		opts.AckTimeout = 100 * time.Millisecond
		opts.Debug = true
		opts.OnClose = func(c *ChannelActive) error {
			close(out)
			return nil
		}
		opts.OnData = func(p *Packet) error {
			out <- *p
			return nil
		}
	})

	return out, channel
}

func newTestChannelPairWithRouter(entityIdL uint32, channelIdL uint16, entityIdR uint32, channelIdR uint16, router func(chan Packet, chan Packet, *ChannelActive, *ChannelActive)) (*ChannelActive, *ChannelActive) {
	outL, channelL := newTestChannel(entityIdL, channelIdL, entityIdR, channelIdR, false)
	outR, channelR := newTestChannel(entityIdR, channelIdR, entityIdL, channelIdL, true)

	go func() {
		router(outL, outR, channelL, channelR)
	}()

	return channelL, channelR
}

func newTestChannelPair(entityIdL uint32, channelIdL uint16, entityIdR uint32, channelIdR uint16) (*ChannelActive, *ChannelActive) {
	return newTestChannelPairWithRouter(entityIdL, channelIdL, entityIdR, channelIdR, func(outL chan Packet, outR chan Packet, channelL *ChannelActive, channelR *ChannelActive) {
		for {
			select {
			case p, ok := <-outR:
				if !ok {
					return
				}
				channelL.log("Routing packet: %v", &p)
				if err := channelL.send(&p); err != nil {
					return
				}
			case p, ok := <-outL:
				if !ok {
					return
				}
				channelR.log("Routing packet: %v", &p)
				if err := channelR.send(&p); err != nil {
					return
				}
			default:
				time.Sleep(5 * time.Millisecond)
			}
		}
	})
}

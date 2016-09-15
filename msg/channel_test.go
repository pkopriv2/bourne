package msg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestActiveChannel_openInitTimeout(t *testing.T) {
	_, channel := newTestChannel(0, 0, 1, 1, false)
	defer channel.Close()

	channel.state.WaitUntil(ChannelOpened | ChannelFailure)

	assert.Equal(t, ChannelFailure, channel.state.Get())
}

func TestActiveChannel_openRecvTimeout(t *testing.T) {
	_, channel := newTestChannel(0, 0, 1, 1, true)
	defer channel.Close()

	// start the sequence, but never respond
	channel.send(NewPacket(channel.session.Local(), channel.session.Remote(), PacketFlagOpen, 100, 0, []byte{}))
	channel.state.WaitUntil(ChannelOpened | ChannelFailure)

	assert.Equal(t, ChannelFailure, channel.state.Get())
}

func TestActiveChannel_openHandshake(t *testing.T) {
	channelL, channelR := newTestChannelPair(0, 0, 1, 1)
	defer channelL.Close()
	defer channelR.Close()

	channelL.state.WaitUntil(ChannelOpened | ChannelFailure)
	channelR.state.WaitUntil(ChannelOpened | ChannelFailure)

	assert.Equal(t, ChannelOpened, channelL.state.Get())
	assert.Equal(t, ChannelOpened, channelR.state.Get())
}

func TestActiveChannel_sendSinglePacket(t *testing.T) {
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

func TestActiveChannel_sendSingleStream(t *testing.T) {
	channelL, channelR := newTestChannelPair(0, 0, 1, 1)
	channelR.debug = false
	defer channelR.Close()
	defer channelL.Close()

	go func() {
		for i := 0; i < 255; i++ {
			if _, err := channelL.Write([]byte{uint8(i)}); err != nil {
				channelL.log("Error writing to channel: %v", err)
				return
			}
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

func TestActiveChannel_sendDuplexStream(t *testing.T) {
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

func TestActiveChannel_sendLargeStream(t *testing.T) {
	channelL, channelR := newTestChannelPair(0, 0, 1, 1)
	defer channelR.Close()
	defer channelL.Close()

	out := make([]byte, 1024)
	for i := 0; i < 1024; i++ {
		out[i] = byte(i)
	}

	go func() {
		for i := 0; i < 1<<10; i++ {
			channelL.Write(out)
		}
	}()

	timer := time.NewTimer(time.Second * 45)

	buf := make([]byte, 1<<14)
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

func newTestChannel(entityIdL EntityId, channelIdL uint16, entityIdR EntityId, channelIdR uint16, listener bool) (chan *packet, *channel) {
	l := NewEndPoint(entityIdL, channelIdL)
	r := NewEndPoint(entityIdR, channelIdR)

	out := make(chan *packet, 1<<10)

	channel := newChannel(l, r, listener, func(opts *ChannelOptions) {
		opts.OnClose = func(c Channel) error {
			close(out)
			return nil
		}
		opts.OnData = func(p *packet) error {
			out <- p
			return nil
		}
	})

	channel.debug = true
	channel.ackTimeout = 500 * time.Millisecond
	channel.closeTimeout = 500 * time.Millisecond
	channel.sendWait = 10 * time.Millisecond
	channel.recvWait = 1 * time.Millisecond

	return out, channel
}

func newTestRouter() func(outL chan *packet, outR chan *packet, channelL *channel, channelR *channel) {
	return func(outL chan *packet, outR chan *packet, channelL *channel, channelR *channel) {
		for {
			select {
			case p, ok := <-outR:
				if !ok {
					return
				}
				channelL.log("Routing packet: %v", p)
				if err := channelL.send(p); err != nil {
					return
				}
			case p, ok := <-outL:
				if !ok {
					return
				}
				channelR.log("Routing packet: %v", p)
				if err := channelR.send(p); err != nil {
					return
				}
			}
		}
	}
}

func newTestChannelPairWithRouter(entityIdL EntityId, channelIdL uint16, entityIdR EntityId, channelIdR uint16, router func(chan *packet, chan *packet, *channel, *channel)) (*channel, *channel) {
	outL, channelL := newTestChannel(entityIdL, channelIdL, entityIdR, channelIdR, false)
	outR, channelR := newTestChannel(entityIdR, channelIdR, entityIdL, channelIdL, true)

	go router(outL, outR, channelL, channelR)

	return channelL, channelR
}

func newTestChannelPair(entityIdL EntityId, channelIdL uint16, entityIdR EntityId, channelIdR uint16) (*channel, *channel) {
	return newTestChannelPairWithRouter(entityIdL, channelIdL, entityIdR, channelIdR, newTestRouter())
}

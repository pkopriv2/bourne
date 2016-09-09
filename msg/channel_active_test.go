package msg

import (
	// "fmt"
	// "sync"
	// "fmt"
	"testing"
	"time"
	// "time"

	"github.com/stretchr/testify/assert"
)
func NewChannelPairWithRouter(entityIdR uint32, channelIdR uint16, entityIdL uint32, channelIdL uint16, router func(chan Packet, chan Packet, *ChannelActive, *ChannelActive)) (*ChannelActive, *ChannelActive) {
	r := ChannelAddress{entityIdR, channelIdR}
	l := ChannelAddress{entityIdL, channelIdL}

	outR := make(chan Packet, 1024)
	outL := make(chan Packet, 1024)

	var channelL, channelR *ChannelActive
	channelR, _ = NewChannelActive(r, l, func(opts *ChannelOptions) {
		opts.Debug = true

		opts.OnClose = func(c *ChannelActive) error {
			close(outR)
			return nil
		}

		opts.OnData = func(p *Packet) error {
			outR <- *p
			return nil
		}
	})

	channelL, _ = NewChannelActive(l, r, func(opts *ChannelOptions) {
		opts.Debug = true
		opts.OnClose = func(c *ChannelActive) error {
			close(outL)
			return nil
		}
		opts.OnData = func(p *Packet) error {
			outL <- *p
			return nil
		}
	})

	go func() {
		router(outL, outR, channelL, channelR)
	}()

	return channelL, channelR
}

func NewChannelPair(entityIdR uint32, channelIdR uint16, entityIdL uint32, channelIdL uint16) (*ChannelActive, *ChannelActive) {
	return NewChannelPairWithRouter(entityIdR, channelIdR, entityIdL, channelIdL, func(outL chan Packet, outR chan Packet, channelL *ChannelActive, channelR *ChannelActive) {
		for {
			select {
			case p, ok := <-outR:
				if !ok {
					return
				}
				if err := channelL.Send(&p); err != nil {
					return
				}
			case p, ok := <-outL:
				if !ok {
					return
				}
				if err := channelR.Send(&p); err != nil {
					return
				}
			default:
				time.Sleep(5 * time.Millisecond)
			}
		}
	})
}

func TestChannelActive_sendSinglePacket(t *testing.T) {
	channelL, channelR := NewChannelPair(0, 0, 1, 1)
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
	channelL, channelR := NewChannelPair(0, 0, 1, 1)
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
	channelL, channelR := NewChannelPair(0, 0, 1, 1)
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
	channelL, channelR := NewChannelPair(0, 0, 1, 1)
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
		if tot >= 1 << 20 {
			break
		}
	}

	assert.Equal(t, 1 << 20, tot)
}

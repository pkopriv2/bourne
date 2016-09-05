package msg

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLogBuffer_init(t *testing.T) {
	buf := NewAckLog(8)

	assert.Equal(t, []byte{}, buf.Data())
	assert.Equal(t, uint32(0), buf.WritePos())
	assert.Equal(t, uint32(0), buf.AckPos())
}

func TestLogBuffer_TryWrite_One(t *testing.T) {
	buf := NewAckLog(4)
	assert.Equal(t, uint32(1), buf.TryWrite([]byte{1}))
	assert.Equal(t, []byte{1}, buf.Data())
	assert.Equal(t, uint32(1), buf.WritePos())
	assert.Equal(t, uint32(0), buf.AckPos())
}

func TestLogBuffer_TryWrite_ToCapacity(t *testing.T) {
	buf := NewAckLog(4)

	assert.Equal(t, uint32(4), buf.TryWrite([]byte{1, 2, 3, 4}))
	assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())
	assert.Equal(t, uint32(4), buf.WritePos())
	assert.Equal(t, uint32(0), buf.AckPos())
}

func TestLogBuffer_TryWrite_BeyondCapacity(t *testing.T) {
	buf := NewAckLog(4)

	assert.Equal(t, uint32(4), buf.TryWrite([]byte{1, 2, 3, 4}))
	assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())
	assert.Equal(t, uint32(4), buf.WritePos())
	assert.Equal(t, uint32(0), buf.AckPos())

	assert.Equal(t, uint32(0), buf.TryWrite([]byte{4, 5, 6}))
	assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())
	assert.Equal(t, uint32(4), buf.WritePos())
	assert.Equal(t, uint32(0), buf.AckPos())
}

func TestLogBuffer_Write_LessThanCapacity(t *testing.T) {
	buf := NewAckLog(4)

	num, err := buf.Write([]byte{1})
	assert.Equal(t, 1, num)
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte{1}, buf.Data())
	assert.Equal(t, uint32(1), buf.WritePos())
	assert.Equal(t, uint32(0), buf.AckPos())
}

func TestLogBuffer_Write_EqualToCapacity(t *testing.T) {
	buf := NewAckLog(4)

	num, err := buf.Write([]byte{1, 2, 3, 4})
	assert.Equal(t, 4, num)
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())
	assert.Equal(t, uint32(4), buf.WritePos())
	assert.Equal(t, uint32(0), buf.AckPos())
}

func TestLogBuffer_Write_GreaterCapacity(t *testing.T) {
	buf := NewAckLog(4)
	var writeEnd time.Time

	wait := new(sync.WaitGroup)
	wait.Add(1)
	go func() {
		buf.Write([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})
		writeEnd = time.Now()
		wait.Done()
	}()

	time.Sleep(100)
	readBeg := time.Now()

	readBuf := make([]byte, 9)
	io.ReadFull(buf, readBuf)

	wait.Wait()
	assert.True(t, writeEnd.After(readBeg))
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, readBuf)
}

func TestActiveChannel_simple(t *testing.T) {
	pool := NewIdPool()
	cache := NewChannelCache()

	l := ChannelAddress{0, 0}
	r := ChannelAddress{1, 0}

	out1 := make(chan Packet, 8)
	out2 := make(chan Packet, 8)

	channel1, _ := NewActiveChannel(l, r, cache, pool, out1)
	channel2, _ := NewActiveChannel(r, l, cache, pool, out2)

	wait := new(sync.WaitGroup)
	wait.Add(1)
	go func() {
		defer wait.Done()
		for {
			select {
			case p1, ok := <-out1:
				if ! ok {
					return
				}

				channel2.Send(&p1)
				break
			case p2, ok := <-out2:
				if ! ok {
					return
				}

				channel1.Send(&p2)
				break
			}
		}
	}()

	wait.Add(1)
	go func() {
		defer wait.Done()
		for {
			buf := make([]byte, 8)

			num, err := channel1.Read(buf)
			if err != nil {
				fmt.Printf("Channel1 closed: %+v\n", err)
				return
			}

			fmt.Printf("Channel1 read %+v\n", buf[:num])
		}
	}()

	wait.Add(1)
	go func() {
		defer wait.Done()
		for i := 0;; i++ {

			_, err := channel2.Write([]byte{byte(i)})
			if err != nil {
				fmt.Printf("Channel2 closed: %+v\n", err)
				return
			}

			time.Sleep(1000 * time.Millisecond)
		}
	}()

	time.Sleep(5 * time.Second)
	time.Sleep(5 * time.Second)
	close(out1)
	close(out2)
	wait.Wait()

}

// func TestActiveChannel_forceBuffer(t *testing.T) {
	// pool := NewIdPool()
	// cache := NewChannelCache()
	// out := make(chan Packet)
//
	// r1 := ChannelAddress{1, 0}
	// channel1, _ := NewActiveChannel(1, r2, cache, pool, out)
//
	// channel2, _ := NewActiveChannel(1, r2, cache, pool, out)
	// r2 := ChannelAddress{2, 0}
	// if err != nil {
		// panic("AA")
	// }
//
	// wait := new(sync.WaitGroup)
	// wait.Add(1)
	// go func() {
		// defer wait.Done()
		// for {
			// _, ok := <-out
			// if !ok {
				// break
			// }
//
			// // fmt.Printf("Received packet: %+v\n", p)
		// }
	// }()
//
	// now := time.Now()
	// for i := 0; i<32000; i++ {
		// channel.Write([]byte{uint8(i)})
	// }
	// fmt.Printf("Took: [%+v]\n", time.Since(now))
//
	// time.Sleep(5 * time.Second)
	// time.Sleep(5 * time.Second)
	// close(out)
	// wait.Wait()
//
// }

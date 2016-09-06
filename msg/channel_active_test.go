package msg

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
	"os"

	"github.com/stretchr/testify/assert"
	"github.com/rcrowley/go-metrics"
)

func TestAckLog_init(t *testing.T) {
	buf := NewDataLog(8)

	assert.Equal(t, []byte{}, buf.Data())
	assert.Equal(t, uint32(0), buf.StartPos().offset)
	assert.Equal(t, uint32(0), buf.ReadPos().offset)
	assert.Equal(t, uint32(0), buf.WritePos().offset)
}


func TestAckLog_TryWrite_One(t *testing.T) {
	buf := NewDataLog(4)

	num, _ := buf.TryWrite([]byte{1})

	assert.Equal(t, uint32(1), num)
	assert.Equal(t, []byte{1}, buf.Data())
	assert.Equal(t, uint32(0), buf.StartPos().offset)
	assert.Equal(t, uint32(0), buf.ReadPos().offset)
	assert.Equal(t, uint32(1), buf.WritePos().offset)
}
//
// func TestAckLog_TryWrite_ToCapacity(t *testing.T) {
// buf := NewAckLog(4)
//
// assert.Equal(t, uint32(4), buf.TryWrite([]byte{1, 2, 3, 4}))
// assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())
// assert.Equal(t, uint32(4), buf.WritePos())
// assert.Equal(t, uint32(0), buf.AckPos())
// }
//
// func TestAckLog_TryRead_None(t *testing.T) {
// buf := NewAckLog(4)
//
// num, beg := buf.TryRead([]byte{0})
// assert.Equal(t, uint32(0), num)
// assert.Equal(t, uint32(0), beg)
// assert.Equal(t, uint32(0), buf.AckPos())
// assert.Equal(t, uint32(0), buf.CurPos())
// }
//
// //
// func TestAckLog_TryWrite_BeyondCapacity(t *testing.T) {
// buf := NewAckLog(4)
//
// assert.Equal(t, uint32(4), buf.TryWrite([]byte{1, 2, 3, 4}))
// assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())
// assert.Equal(t, uint32(4), buf.WritePos())
// assert.Equal(t, uint32(0), buf.AckPos())
//
// assert.Equal(t, uint32(0), buf.TryWrite([]byte{4, 5, 6}))
// assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())
// assert.Equal(t, uint32(4), buf.WritePos())
// assert.Equal(t, uint32(0), buf.AckPos())
// }
//
// func TestAckLog_Write_LessThanCapacity(t *testing.T) {
// buf := NewAckLog(4)
//
// num, err := buf.Write([]byte{1})
// assert.Equal(t, 1, num)
// assert.Equal(t, nil, err)
// assert.Equal(t, []byte{1}, buf.Data())
// assert.Equal(t, uint32(1), buf.WritePos())
// assert.Equal(t, uint32(0), buf.AckPos())
// }
//
// func TestAckLog_Write_EqualToCapacity(t *testing.T) {
// buf := NewAckLog(4)
//
// num, err := buf.Write([]byte{1, 2, 3, 4})
// assert.Equal(t, 4, num)
// assert.Equal(t, nil, err)
// assert.Equal(t, []byte{1, 2, 3, 4}, buf.Data())
// assert.Equal(t, uint32(4), buf.WritePos())
// assert.Equal(t, uint32(0), buf.AckPos())
// }
//
// func TestAckLog_Write_GreaterCapacity(t *testing.T) {
// buf := NewAckLog(4)
// var writeEnd time.Time
//
// wait := new(sync.WaitGroup)
// wait.Add(1)
// go func() {
// buf.Write([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})
// writeEnd = time.Now()
// wait.Done()
// }()
//
// time.Sleep(100)
// readBeg := time.Now()
//
// readBuf := make([]byte, 9)
// io.ReadFull(buf, readBuf)
//
// wait.Wait()
// assert.True(t, writeEnd.After(readBeg))
// assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, readBuf)
// }
//
func TestActiveChannel_simple(t *testing.T) {

	l := ChannelAddress{0, 0}
	r := ChannelAddress{1, 0}

	out1 := make(chan Packet, 1024)
	out2 := make(chan Packet, 1024)

	channel1, _ := NewChannelActive(l, r, out1)
	channel2, _ := NewChannelActive(r, l, out2)

	wait := new(sync.WaitGroup)
	wait.Add(1)
	go func() {
		defer wait.Done()
		// sent := 0
		for i := 0; ; i++ {
			select {
			case p1, ok := <-out1:
				if !ok {
					return
				}

				// sent += len(p1.data)
				// if i%5 == 0 {
					// fmt.Printf("-- TOTAL ROUTED ---: %+v KB \n", float64((sent * 1.0)) / float64((1 << 10)))
				// }

				// if i%100 == 2 {
					// // fmt.Printf("-- DROPPED PACKET ---: %+v\n", i)
					// break
				// }


				// fmt.Printf("-- [ROUTE] CHAN1->CHAN2 ---: %v\n", i)
				channel2.Send(&p1)
			case p2, ok := <-out2:
				if !ok {
					return
				}

				// sent += len(p2.data)
				// if i%5 == 0 {
					// fmt.Printf("-- TOTAL ROUTED ---: %+v KB \n", float64((sent * 1.0)) / float64((1 << 10)))
				// }

				// if i%100 == 2 {
					// // fmt.Printf("-- DROPPED PACKET ---: %+v\n", i)
					// break
				// }

				// fmt.Printf("-- [ROUTE] CHAN2->CHAN1 ---: %v\n", i)
				channel1.Send(&p2)
			}
		}
	}()

	wait.Add(1)
	go func() {
		defer wait.Done()

		for i := 0;;{
			buf := make([]byte, 1024)

			num, err := channel1.Read(buf)
			if err != nil {
				fmt.Printf("Channel1 closed: %+v\n", err)
				return
			}

			for range buf[:num] {
				// fmt.Printf("Channel1 read %+v\n", i)
				i++
			}
		}
	}()

	wait.Add(1)
	go func() {
		defer wait.Done()
		for i := 0; i < 10000000; i++ {

			_, err := channel2.Write([]byte{byte(i)})
			if err != nil {
				fmt.Printf("Channel2 closed: %+v\n", err)
				return
			}

			// fmt.Printf("Channel2 write %+v\n", []byte{byte(i)})
			// time.Sleep(20 * time.Millisecond)
		}
	}()

	go metrics.Log(metrics.DefaultRegistry, 1 * time.Second, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

	time.Sleep(5 * time.Second)
	fmt.Println("sleeping")
	time.Sleep(5 * time.Second)
	fmt.Println("sleeping")

	time.Sleep(1 * time.Second)
	fmt.Println("sleeping")
	close(out1)
	close(out2)
	wait.Wait()


}

//
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

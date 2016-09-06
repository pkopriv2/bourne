package msg

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestActiveChannel_simple(t *testing.T) {

	l := ChannelAddress{0, 0}
	r := ChannelAddress{1, 0}

	out1 := make(chan Packet, 1024)
	out2 := make(chan Packet, 1024)

	channel1, _ := NewChannelActive(l, r, out1)
	channel2, _ := NewChannelActive(r, l, out2, func(opts *ChannelOptions) {
		opts.SendDebug = true
	})

	wait := new(sync.WaitGroup)
	wait.Add(1)
	go func() {
		defer wait.Done()
		// sent := 0
		for i := 0; ; i++ {
			select {
			case p, ok := <-out1:
				if !ok {
					return
				}

				// sent += len(p.data)
				// if i%5 == 0 {
					// fmt.Printf("-- TOTAL ROUTED ---: %+v KB \n", float64((sent * 1.0)) / float64((1 << 10)))
				// }

				if i%10 == 2 {
					fmt.Printf("-- DROPPED PACKET ---: %+v\n", i)
					break
				}

				fmt.Printf("-- [ROUTE] CHAN1->CHAN2 ---: %v : offset: %+v\n", i, p.offset)
				channel2.Send(&p)
			case p, ok := <-out2:
				if !ok {
					return
				}

				// sent += len(p.data)
				// if i%5 == 0 {
					// fmt.Printf("-- TOTAL ROUTED ---: %+v KB \n", float64((sent * 1.0)) / float64((1 << 10)))
				// }

				if i%10 == 2 {
					fmt.Printf("-- DROPPED PACKET ---: %+v\n", i)
					break
				}

				fmt.Printf("-- [ROUTE] CHAN2->CHAN1 ---: %v : offset: %+v\n", i, p.offset)
				channel1.Send(&p)
			}
		}
	}()

	wait.Add(1)
	go func() {
		defer wait.Done()

		for i := 0; ; {
			buf := make([]byte, 1024)

			num, err := channel1.Read(buf)
			if err != nil {
				fmt.Printf("Channel1 closed: %+v\n", err)
				return
			}

			for _, val := range buf[:num] {
				fmt.Printf("Channel1 read %+v:  %v\n", i, val)
				i++
			}
		}
	}()

	wait.Add(1)
	go func() {
		defer wait.Done()
		for i := 0; i < 100; i++ {

			_, err := channel2.Write([]byte{byte(i)})
			if err != nil {
				fmt.Printf("Channel2 closed: %+v\n", err)
				return
			}

			// fmt.Printf("Channel2 write %+v\n", []byte{byte(i)})
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// go metrics.Log(metrics.DefaultRegistry, 1 * time.Second, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

	fmt.Printf("Metrics: %+v", channel1.stats)
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

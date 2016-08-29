package msg

import "testing"
//import "bytes"
//import "bufio"
import "io"
import "fmt"
import "time"
import "sync"

//import "github.com/pkopriv2/bourne/msg"

func TestIdPool(*testing.T) {
    pool := NewIdPool()

    // go func() {
        // // this thread will only take values
        // for {
            // val, err := pool.Take()
            // if err != nil {
                // fmt.Println(err)
                // break
            // }
//
            // fmt.Printf("[thread-2] [%v]\n", val)
            // time.Sleep(time.Millisecond * 500)
        // }
    // }()

    var val uint16
    // var err error
    val, _ = pool.Take()
    fmt.Printf("take: %v\n", val)

    ret, _ := pool.Take()
    fmt.Printf("to return val: %v\n", ret)

    val, _ = pool.Take()
    fmt.Printf("take: %v\n", val)

    pool.Return(ret)
    fmt.Printf("returned: %v\n", ret)
    val, _ = pool.Take()
    fmt.Printf("take: %v\n", val)
    val, _ = pool.Take()
    fmt.Printf("take: %v\n", val)
}

func TestChannel(*testing.T) {

    out := make(chan Packet)

    l := ChannelAddress{0,0}
    r := ChannelAddress{1,1}

    channel := NewChannel(l, r, out)

    wait := new(sync.WaitGroup)
    go func(channel *Channel) {
        wait.Add(1)
        for i := 0; ; i++ {
            if err := channel.Send(&Packet { 0,1,2,3,4,5, []byte{byte(i)}}); err != nil {
                fmt.Printf("%v\n", err)
                break;
            }

            time.Sleep(time.Millisecond * 10)
        }
        wait.Done()
    }(channel)

    for i := 0; i<10; i++{
        // reader := channel.Reader();
        buf := make([]byte, 7)
        io.ReadFull(channel, buf)
        fmt.Printf("%v\n", buf)
        time.Sleep(time.Second)
    }

    channel.Close();
    wait.Wait()
}


func TestMultiplexEndPoint2(*testing.T) {
    //c1 := NewMemConnection();
    //c2 := NewMemConnection();


    //m1 := NewMultiplexer(c1, c2)
    ////m2 := NewMultiplexer(c2, c1)

    ////buffer := bytes.NewBuffer(make([]byte, 0))
    ////writer := bufio.NewWriter(buffer)

    ////msg1 := &Datagram { 0,1,2,3,4,5, make([]byte, 1) }
    ////msg1.write(c1)
    ////fmt.Println(msg1)

    ////fmt.Println(m1)
    ////fmt.Println(m2)

    //for {
        //fmt.Println("sleeping")
        //time.Sleep(time.Second)
        //m1.shutdown()
    //}
}

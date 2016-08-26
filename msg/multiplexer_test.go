package msg

import "testing"
//import "bytes"
//import "bufio"
import "fmt"
import "time"

//import "github.com/pkopriv2/bourne/msg"

func TestMultiplexEndPoint2(*testing.T) {
    c1 := NewMemConnection();
    c2 := NewMemConnection();


    m1 := NewMultiplexer(c1, c2)
    //m2 := NewMultiplexer(c2, c1)

    //buffer := bytes.NewBuffer(make([]byte, 0))
    //writer := bufio.NewWriter(buffer)

    msg1 := &Datagram { 0,1,2,3,4,5, make([]byte, 1) }
    msg1.write(c1)
    fmt.Println(msg1)

    //fmt.Println(m1)
    //fmt.Println(m2)

    for {
        fmt.Println("sleeping")
        time.Sleep(time.Second)
        m1.shutdown()
    }
}

package msg

import "testing"
import "bytes"
import "bufio"
import "fmt"
import "time"

//import "github.com/pkopriv2/bourne/p"

func TestSerialization(*testing.T) {
    buffer := bytes.NewBuffer(make([]byte, 0))
    writer := bufio.NewWriter(buffer)

    p1 := &Packet { 0,1,2,3,4,5, make([]byte, 20) }
    WritePacket(writer, p1)
    writer.Flush()

    fmt.Println()
    fmt.Printf("%v\n", buffer.Bytes())
    fmt.Println()
    fmt.Println()


    reader := bufio.NewReader(buffer)

    p2, _ := ReadPacket(reader)
    fmt.Printf("%v\n", p1)
    fmt.Printf("%v\n", p2)
}

func TestPacketReader(*testing.T) {

    c := make(chan Packet)

    go func(c chan<- Packet) {
        for {
            p := &Packet { 0,1,2,3,4,5, []byte{0,1,2,3,4,5,6,7,8,9}}
            c<- *p;
        }
    }(c)

    reader := NewPacketReader(c)

    for {
        buf := make([]byte, 100)
        reader.Read(buf)
        fmt.Printf("%v\n", buf)

        time.Sleep(time.Second)
    }
}

func TestPacketWriter(*testing.T) {

    //c := make(chan Packet)

    //go func(c <-chan<- Packet) {
        //for {
            //p := &Packet { 0,1,2,3,4,5, []byte{0,1,2,3,4,5,6,7,8,9}}
            //c<- *p;
        //}
    //}(c)

    //reader := NewPacketWr(c)

    //for {
        //buf := make([]byte, 100)
        //reader.Read(buf)
        //fmt.Printf("%v\n", buf)

        //time.Sleep(time.Second)
    //}
}

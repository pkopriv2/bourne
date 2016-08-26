package msg

import "testing"
import "bytes"
import "bufio"
import "fmt"

//import "github.com/pkopriv2/bourne/msg"

func TestSerialization(*testing.T) {
    buffer := bytes.NewBuffer(make([]byte, 0))
    writer := bufio.NewWriter(buffer)

    msg1 := &Datagram { 0,1,2,3,4,5, make([]byte, 20) }
    msg1.write(writer)
    writer.Flush()

    fmt.Println()
    fmt.Printf("%v\n", buffer.Bytes())
    fmt.Println()
    fmt.Println()


    reader := bufio.NewReader(buffer)

    msg2, _ := readDatagram(reader)
    fmt.Printf("%v\n", msg1)
    fmt.Printf("%v\n", msg2)
}

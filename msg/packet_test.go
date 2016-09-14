package msg

import "testing"
import "bytes"
import "bufio"
import "fmt"

// import "time"

func TestPacket_Serialization(*testing.T) {
	buffer := bytes.NewBuffer(make([]byte, 0))
	writer := bufio.NewWriter(buffer)

	src := NewEndPoint(0, 0)
	dst := NewEndPoint(1, 1)
	p1 := &packet{0, src, dst, 5, 0, 0, make([]byte, 20)}
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

//func TestPacketReader(*testing.T) {

//c := make(chan Packet)

//go func(c chan<- Packet) {
//for {
//p := &Packet { 0,1,2,3,4,5, []byte{0,1,2,3,4,5,6,7,8,9}}
//c<- *p;
//}
//}(c)

//reader := NewPacketReader(c)

//for {
//buf := make([]byte, 7)

//reader.Read(buf)
//fmt.Printf("%v\n", buf)

//time.Sleep(time.Second)
//}
//}

// func TestPacketWriter(*testing.T) {
//
// c := make(chan Packet)
//
// go func(c chan<- Packet) {
// writer := NewPacketWriter(c, 1,1, 2,2)
//
// for i := 0; ; i++{
// writer.Write(make([]byte, 1+i))
// }
// }(c)
//
//
// for {
// p := <-c
// fmt.Printf("%v\n", p)
//
// time.Sleep(time.Second)
// }
// }

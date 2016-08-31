package msg

import "testing"

//import "bytes"
//import "bufio"
// import "io"
import "fmt"

// import "time"
// import "sync"

//import "github.com/pkopriv2/bourne/msg"

func TestListenerChannel_NewListenerChannel(t *testing.T) {
	cache := NewChannelCache()
	ids := NewIdPool()

	out := make(chan Packet)

	listener, err := NewChannelListener(&ChannelAddress{0,0}, cache, ids, out)
	listener.Send(&Packet{0, 1, 1, 0, 0, 1, []byte{1,2,3}})

	len(cache.channels)

	// p1 := &Packet{0, 1, 1, 0, 0, 1, []byte{1,2,3}}

	// assert.Equal(t, 123, 123, "they should be equal")
	// fmt.Println("hello")
}

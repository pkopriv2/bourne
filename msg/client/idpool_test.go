package client

import "testing"
//import "bytes"
//import "bufio"
// import "io"
import "fmt"

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

    var val uint
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

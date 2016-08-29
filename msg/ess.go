package msg

// // A full duplex stream abstraction.
// //
// type DuplexStream struct {
    // Reader: io.ReadCloser,
    // Writer: io.WriteCloser
// }
//
// //
// //
// //
// type EntityStreamService interface {
    // Lookup(entityId uint32) (Stream, error)
// }
//
// type MemStream struct {
    // inner chan byte
// }
//
// func NewMemStream() *MemStream {
    // return &MemStream{make(chan byte)}
// }
//
// func (s *MemStream) Read(p []byte) (n int, err error) {
    // length := len(p)
//
    // buffer := bytes.NewBuffer(p)
    // writer := bufio.NewWriter(buffer)
    // buffer.Reset()
//
    // for i := 0; i < length; i++ {
        // writer.Write([]byte{<-s.inner})
    // }
//
    // writer.Flush()
    // return length, nil
// }
//
// func (s *MemStream) Write(p []byte) (n int, err error) {
    // length := len(p)
//
    // for i := 0; i < length; i++ {
        // s.inner<- byte(p[i])
    // }
//
    // return length, nil
// }

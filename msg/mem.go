package msg

import "bytes"
import "bufio"

//import "log"

type MemConnection struct {
	inner chan byte
}

func NewMemConnection() *MemConnection {
	return &MemConnection{make(chan byte)}
}

func (s *MemConnection) Read(p []byte) (n int, err error) {
	length := len(p)

	buffer := bytes.NewBuffer(p)
	writer := bufio.NewWriter(buffer)
	buffer.Reset()

	for i := 0; i < length; i++ {
		writer.Write([]byte{<-s.inner})
	}

	writer.Flush()
	return length, nil
}

func (s *MemConnection) Write(p []byte) (n int, err error) {
	length := len(p)

	for i := 0; i < length; i++ {
		s.inner <- byte(p[i])
	}

	return length, nil
}

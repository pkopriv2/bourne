package wire

import "fmt"

// A address is the memory-address of a conversation between entities
type Address interface {
	Src() EndPoint
	Dst() EndPoint
}

type address struct {
	src EndPoint
	dst EndPoint // nil for listeners.
}
func NewLocalAddress(src EndPoint) Address {
	return address{src, nil}
}

func NewRemoteAddress(src EndPoint, dst EndPoint) Address {
	return address{src, dst}
}

func (s address) Src() EndPoint {
	return s.src
}

func (s address) Dst() EndPoint {
	return s.dst
}

func (s address) String() string {
	return fmt.Sprintf("[%v->%v]", s.src, s.dst)
}

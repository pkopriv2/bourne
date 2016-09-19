package wire

import (
	"fmt"

	uuid "github.com/satori/go.uuid"
)

// A route simply describes the start and stop for a packet
type Route interface {
	Src() EndPoint
	Dst() EndPoint

	Reverse() Route
}

// An endpoint is the tuple necessary to identify one side of a route
type EndPoint interface {
	MemberId() uuid.UUID
	ChannelId() uint64
}

func NewLocalRoute(src EndPoint) Route {
	return route{src, nil}
}

func NewRemoteRoute(src EndPoint, dst EndPoint) Route {
	return route{src, dst}
}

func NewEndPoint(memberId uuid.UUID, channelId uint64) EndPoint {
	return endPoint{memberId, channelId}
}

type route struct {
	src EndPoint
	dst EndPoint // nil for listeners.
}

func (s route) Reverse() Route {
	return route{s.dst, s.src}
}

func (s route) Src() EndPoint {
	return s.src
}

func (s route) Dst() EndPoint {
	return s.dst
}

func (s route) String() string {
	return fmt.Sprintf("[%v->%v]", s.src, s.dst)
}

type endPoint struct {
	memberId  uuid.UUID
	channelId uint64
}

func (c endPoint) MemberId() uuid.UUID {
	return c.memberId
}

func (c endPoint) ChannelId() uint64 {
	return c.channelId
}

func (c endPoint) String() string {
	return fmt.Sprintf("endpoint(%v:%v)", c.memberId, c.channelId)
}

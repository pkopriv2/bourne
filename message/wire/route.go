package wire

import (
	"fmt"

	uuid "github.com/satori/go.uuid"
)

type Routable interface {
	Route() Route
}

// A route simply describes the start and stop for a packet
type Route interface {
	Src() Address
	Dst() Address
	Reverse() Route
	BuildPacket() PacketBuilder
	String() string
}

// An address is the tuple necessary to identify one side of a route
type Address interface {
	MemberId() uuid.UUID
	ChannelId() uint64
}

func NewLocalRoute(src Address) Route {
	return route{src, nil}
}

func NewRemoteRoute(src Address, dst Address) Route {
	return route{src, dst}
}

func NewAddress(memberId uuid.UUID, channelId uint64) Address {
	return address{memberId, channelId}
}

type route struct {
	src Address
	dst Address // nil for listeners.
}

func (s route) BuildPacket() PacketBuilder {
	return BuildPacket(s)
}

func (s route) Reverse() Route {
	return route{s.dst, s.src}
}

func (s route) Src() Address {
	return s.src
}

func (s route) Dst() Address {
	return s.dst
}

func (s route) String() string {
	return fmt.Sprintf("[%v->%v]", s.src, s.dst)
}

type address struct {
	memberId  uuid.UUID
	channelId uint64
}

func (c address) MemberId() uuid.UUID {
	return c.memberId
}

func (c address) ChannelId() uint64 {
	return c.channelId
}

func (c address) String() string {
	return fmt.Sprintf("channel(%v:%v)", c.memberId.String()[:4], c.channelId)
}

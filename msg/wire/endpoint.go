package wire

import (
	"fmt"

	uuid "github.com/satori/go.uuid"
)


// This file defines the addressing primitives in using the bourne protocol.

// An endpoint is the tuple necessary to identify one side of an address
type EndPoint interface {
	MemberId() uuid.UUID
	ChannelId() uint64
}

type endPoint struct {
	entityId  uuid.UUID
	channelId uint64
}

func NewEndPoint(entityId uuid.UUID, channelId uint64) EndPoint {
	return endPoint{entityId, channelId}
}

func (c endPoint) MemberId() uuid.UUID {
	return c.entityId
}

func (c endPoint) ChannelId() uint64 {
	return c.channelId
}

func (c endPoint) String() string {
	return fmt.Sprintf("endpoint(%v:%v)", c.entityId, c.channelId)
}

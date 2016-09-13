package msg

type EndPoint interface {
	EntityId() uint32
	ChannelId() uint16
}

type endPoint struct {
	entityId  uint32
	channelId uint16
}

func NewEndPoint(entityId uint32, channelId uint16) EndPoint {
	return endPoint{entityId, channelId}
}

func (c endPoint) EntityId() uint32 {
	return c.entityId
}

func (c endPoint) ChannelId() uint16 {
	return c.channelId
}

// A session address is the tuple necessary to uniquely identify
// a channel within a shared stream.  All packet routing is done
// based on complete session address
//
type Session interface {
	LocalEndPoint() EndPoint
	RemoteEndPoint() EndPoint // nil for listeners
}

type session struct {
	local  EndPoint
	remote EndPoint // nil for listeners.
}

func (s session) LocalEndPoint() EndPoint {
	return s.local
}

func (s session) RemoteEndPoint() EndPoint {
	return s.remote
}

func NewListenerSession(local EndPoint) Session {
	return session{local, nil}
}

func NewChannelSession(local EndPoint, remote EndPoint) Session {
	return session{local, remote}
}

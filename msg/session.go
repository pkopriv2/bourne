package msg

import "fmt"

// A session is an address to either:
//
//   1. A listener.
//   2. A channel
//
//  More fundamentally, a session is an in-memory address to a
//  "routable" object.  This is primarily used to store and retrieve
//  channels and listeners.
//
type Session interface {

	// The local endpoint.  Never nil.
	Local() EndPoint

	// the remote endpoint.  Nil for listeners.
	Remote() EndPoint
}

type session struct {
	local  EndPoint
	remote EndPoint // nil for listeners.
}

func (s session) Local() EndPoint {
	return s.local
}

func (s session) Remote() EndPoint {
	return s.remote
}

func (s session) String() string {
	return fmt.Sprintf("%v->%v", s.local, s.remote)
}

func NewListenerSession(local EndPoint) Session {
	return session{local, nil}
}

func NewChannelSession(local EndPoint, remote EndPoint) Session {
	return session{local, remote}
}

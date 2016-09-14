package msg

import (
	"fmt"
	"io"
	"sync"
)

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

func NewListenerSession(local EndPoint) Session {
	return session{local, nil}
}

func NewChannelSession(local EndPoint, remote EndPoint) Session {
	return session{local, remote}
}

// Something that is routable contains two components:
//
//   1. A session address (ie a way to store the session)
//   2. A method which accepts packets
//
// *Implementations must be thread-safe*
//
type Routable interface {
	io.Closer

	// Returns the complete session address of this channel.
	Session() Session

	// Sends a packet the processors input channel.  Each implementation
	// should attempt to implement this in a NON-BLOCKING
	// fashion. However, may block if necessary.
	//
	send(p *packet) error
}

// A thread safe channel tracking cache.
//
// *This object is thread-safe.*
//
type routingTable struct {

	// the map locl (pool is thread safe already)
	lock sync.RWMutex

	// channels map
	channels map[Session]Routable
}

func newRoutingTable() *routingTable {
	return &routingTable{channels: make(map[Session]Routable)}
}

func (self *routingTable) Get(session Session) Routable {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.channels[session]
}

func (self *routingTable) Add(routable Routable) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	session := routable.Session()

	ret := self.channels[session]
	if ret != nil {
		return ErrChannelExists
	}

	self.channels[session] = routable
	return nil
}

func (self *routingTable) Remove(session Session) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	ret := self.channels[session]
	if ret == nil {
		return ErrChannelUnknown
	}

	delete(self.channels, session)
	return nil
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

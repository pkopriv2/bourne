package msg

import (
	"sync"
)

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

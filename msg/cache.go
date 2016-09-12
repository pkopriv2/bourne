package msg

import (
	"sync"
)

// A thread safe channel tracking cache.
//
// *This object is thread-safe.*
//
type ChannelCache struct {

	// the map locl (pool is thread safe already)
	lock sync.RWMutex

	// channels map
	channels map[Session]Routable
}

func NewChannelCache() *ChannelCache {
	return &ChannelCache{channels: make(map[Session]Routable)}
}

func (self *ChannelCache) Get(session Session) Routable {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.channels[session]
}

func (self *ChannelCache) Add(routable Routable) error {
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

func (self *ChannelCache) Remove(session Session) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	ret := self.channels[session]
	if ret == nil {
		return ErrChannelUnknown
	}

	delete(self.channels, session)
	return nil
}

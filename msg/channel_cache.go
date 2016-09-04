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
	lock *sync.RWMutex

	// channels map
	channels map[ChannelAddress]ChannelBase
}

func NewChannelCache() *ChannelCache {
	return &ChannelCache{lock: new(sync.RWMutex), channels: make(map[ChannelAddress]ChannelBase) }
}

func (self *ChannelCache) Get(addr ChannelAddress) ChannelBase {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.channels[addr]
}

func (self *ChannelCache) Add(addr ChannelAddress, p ChannelBase) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	ret := self.channels[addr]
	if ret != nil {
		return CHANNEL_EXISTS_ERROR
	}

	self.channels[addr] = p
	return nil
}

func (self *ChannelCache) Remove(addr ChannelAddress) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	ret := self.channels[addr]
	if ret == nil {
		return CHANNEL_UNKNOWN_ERROR
	}

	delete(self.channels, addr)
	return nil
}

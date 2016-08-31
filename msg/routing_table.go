package msg

import (
	"sync"
)

// A thread safe channel tracking cache.
//
// *This object is thread-safe.*
//
type RoutingTable struct {

	// the map locl (pool is thread safe already)
	lock *sync.RWMutex

	// channels map
	channels map[ChannelAddress]PacketProcessor
}

func NewRoutingTable() *RoutingTable {
	return &RoutingTable{lock: new(sync.RWMutex)}
}

func (self *RoutingTable) Get(addr ChannelAddress) PacketProcessor {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.channels[addr]
}

func (self *RoutingTable) Add(addr ChannelAddress, p PacketProcessor) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	ret := self.channels[addr]
	if ret != nil {
		return CHANNEL_EXISTS_ERROR
	}

	self.channels[addr] = p
	return nil
}

func (self *RoutingTable) Remove(addr ChannelAddress) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	ret := self.channels[addr]
	if ret == nil {
		return CHANNEL_UNKNOWN_ERROR
	}

	delete(self.channels, addr)
	return nil
}

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
    channels map[ChannelAddress]RoutableChannel
}

func NewChannelCache() *ChannelCache {
    return &ChannelCache{lock: new(sync.RWMutex)}
}

func (self *ChannelCache) Get(addr ChannelAddress) RoutableChannel {
    self.lock.RLock(); defer self.lock.RUnlock()
    return self.channels[addr]
}

func (self *ChannelCache) Add(c RoutableChannel) error {
    self.lock.Lock(); defer self.lock.Unlock()

    ret := self.channels[c.LocalAddr()]
    if ret != nil {
        return CHANNEL_EXISTS_ERROR
    }

    self.channels[c.LocalAddr()] = c
    return nil;
}

func (self *ChannelCache) Remove(addr ChannelAddress) error {
    self.lock.Lock(); defer self.lock.Unlock()

    ret := self.channels[addr]
    if ret == nil {
        return CHANNEL_UNKNOWN_ERROR
    }

    delete(self.channels, addr)
    return nil
}

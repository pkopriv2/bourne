package client

import (
	"io"
	"sync"

	"github.com/pkopriv2/bourne/msg/wire"
)

// Something that is routable contains two components:
//
type Routable interface {
	io.Closer

	// Returns the complete addr address of this channel.
	Address() wire.Address

	// Sends a packet the processors input channel.  Each implementation
	// should attempt to implement this in a NON-BLOCKING
	// fashion. However, may block if necessary.
	//
	send(p wire.Packet) error
}

// A thread safe channel tracking cache.
//
// *This object is thread-safe.*
//
type routingTable struct {

	// the map locl (pool is thread safe already)
	lock sync.RWMutex

	// channels map
	channels map[wire.Address]Routable
}

func newRoutingTable() *routingTable {
	return &routingTable{channels: make(map[core.Address]Routable)}
}

func (self *routingTable) Get(addr core.Address) Routable {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.channels[addr]
}

func (self *routingTable) Add(routable Routable) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	addr := routable.Address()

	ret := self.channels[addr]
	if ret != nil {
		return ErrChannelExists
	}

	self.channels[addr] = routable
	return nil
}

func (self *routingTable) Remove(addr core.Address) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	ret := self.channels[addr]
	if ret == nil {
		return ErrChannelUnknown
	}

	delete(self.channels, addr)
	return nil
}

package client

import (
	"fmt"
	"io"
	"sync"

	"github.com/pkopriv2/bourne/msg/wire"
)

type Routable interface {
	io.Closer

	Route() wire.Route

	// Sends a packet the processors input channel.  Each implementation
	// should attempt to implement this in a NON-BLOCKING
	// fashion. However, may block if necessary.
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
	channels map[wire.Route]Routable
}

func newRoutingTable() *routingTable {
	return &routingTable{channels: make(map[wire.Route]Routable)}
}

func (r *routingTable) Get(addr wire.Route) Routable {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.channels[addr]
}

func (r *routingTable) Add(routable Routable) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	key := routable.Route()

	ret := r.channels[key]
	if ret != nil {
		return fmt.Errorf("Route exists: %v", key)
	}

	r.channels[key] = routable
	return nil
}

func (r *routingTable) Remove(addr wire.Route) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	ret := r.channels[addr]
	if ret == nil {
		return fmt.Errorf("Route does not exist: %v", addr)
	}

	delete(r.channels, addr)
	return nil
}

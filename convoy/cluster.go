package convoy

import (
	"sync"

	"github.com/pkopriv2/bourne/common"
)

type cluster struct {
	ctx    common.Context
	logger common.Logger
	db     Database

	localHost string
	localPort int

	peerFn func() (*client, error)

	fail     error
	inst     *replica
	instLock sync.RWMutex
}

func (c *cluster) newReplica() (*replica, error) {
	cl, err := c.peerFn()
	if err != nil {
		return nil, err
	}

	if cl == nil {
		return newMasterReplica(c.ctx, c.db, c.localHost, c.localPort)
	}
	defer cl.Close()

	// return newMember(c.ctx, c.db, c.port, cl)
	return nil, nil
}

func (c *cluster) shutdown(reason error) error {
	c.instLock.Lock()
	defer c.instLock.Unlock()
	return nil
}

func (c *cluster) Close() error {
	return nil
}

func (c *cluster) reInit() error {
	c.instLock.Lock()
	defer c.instLock.Unlock()
	return nil
}

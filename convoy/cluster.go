package convoy

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	uuid "github.com/satori/go.uuid"
)

type cluster struct {
	context common.Context
	logger  common.Logger
	port    int
	inst    concurrent.Val
}

func (c *cluster) newReplica() *replica {
	return nil
	// return c.inst.Get()
}

func (c *cluster) Replica() *replica {
	return nil
}

func (c *cluster) Close() error {
	return nil
}

func (c *cluster) GetMember(id uuid.UUID) (Member, error) {
	return nil, nil
}

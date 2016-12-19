package kayak

import (
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

type client struct {
	raw net.Connection
}

func (c *client) AppendEvents(id uuid.UUID, term int, commit int, logIndex int, logTerm int, batch []event) (response, error) {
	panic("")
}

func (c *client) Append(batch []event) error {
	panic("")
}

func (c *client) RequestVote(id uuid.UUID, term int, logIndex int, logTerm int) (response, error) {
	panic("")
}

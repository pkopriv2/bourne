package kayak

import (
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

type client struct {
	raw net.Client
}

func (c *client) AppendEvents(id uuid.UUID, term int, logIndex int, logTerm int, batch []event, commit int) (response, error) {
	resp, err := c.raw.Send(newAppendEventsRequest(appendEventsRequest{id, term, batch, logIndex, logTerm, commit}))
	if err != nil {
		return response{}, err
	}

	return readResponseResponse(resp)
}

func (c *client) Append(batch []event) error {
	panic("")
}

func (c *client) RequestVote(id uuid.UUID, term int, logIndex int, logTerm int) (response, error) {
	resp, err := c.raw.Send(newRequestVoteRequest(requestVoteRequest{id, term, logIndex, logTerm}))
	if err != nil {
		return response{}, err
	}

	return readResponseResponse(resp)
}

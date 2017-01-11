package kayak

import (
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

type client struct {
	raw    net.Client
	parser Parser
}

func newClient(raw net.Client, p Parser) *client {
	return &client{raw, p}
}

func (c *client) Close() error {
	return c.raw.Close()
}

func (c *client) AppendEvents(id uuid.UUID, term int, logIndex int, logTerm int, batch []Event, commit int) (response, error) {
	resp, err := c.raw.Send(newAppendEventsRequest(appendEventsRequest{id, term, batch, logIndex, logTerm, commit}))
	if err != nil {
		return response{}, err
	}

	return readResponseResponse(resp)
}

func (c *client) Append(e Event) (LogItem, error) {
	resp, err := c.raw.Send(newAppendRequest(e))
	if err != nil {
		return LogItem{}, err
	}

	index, term, err := readAppendResponse(resp)
	if err != nil {
		return LogItem{}, err
	}

	return LogItem{index, e, term}, nil
}

func (c *client) RequestVote(id uuid.UUID, term int, logIndex int, logTerm int) (response, error) {
	resp, err := c.raw.Send(newRequestVoteRequest(requestVoteRequest{id, term, logIndex, logTerm}))
	if err != nil {
		return response{}, err
	}

	return readResponseResponse(resp)
}

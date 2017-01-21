package kayak

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

type client struct {
	raw net.Client
}

func newClient(raw net.Client) *client {
	return &client{raw}
}

func (c *client) Close() error {
	return c.raw.Close()
}

func (c *client) Replicate(id uuid.UUID, term int, logIndex int, logTerm int, batch []LogItem, commit int) (response, error) {
	resp, err := c.raw.Send(newReplicateRequest(replicateRequest{id, term, batch, logIndex, logTerm, commit}))
	if err != nil {
		return response{}, errors.Wrapf(err, "Error sending replicate [prevIndex=%v,term=%v,num=%v]", logIndex, logTerm, len(batch))
	}

	return readResponseResponse(resp)
}

func (c *client) Append(e Event) (LogItem, error) {
	resp, err := c.raw.Send(newAppendRequest(e))
	if err != nil {
		return LogItem{}, errors.Wrapf(err, "Error sending client append: %v", e)
	}

	index, term, err := readAppendResponse(resp)
	if err != nil {
		return LogItem{}, errors.Wrapf(err, "Error receiving append response: %v", e)
	}

	return newEventLogItem(index, term, e), nil
}

func (c *client) RequestVote(id uuid.UUID, term int, logIndex int, logTerm int) (response, error) {
	resp, err := c.raw.Send(newRequestVoteRequest(requestVoteRequest{id, term, logIndex, logTerm}))
	if err != nil {
		return response{}, errors.Wrapf(err, "Error sending request vote")
	}

	return readResponseResponse(resp)
}

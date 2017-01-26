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

// func (c *client) UpdateRoster(peer peer, join bool) (bool, error) {
	// resp, err := c.raw.Send(replicateRequest{id, term, batch, logIndex, logTerm, commit}.Request())
	// if err != nil {
		// return response{}, errors.Wrapf(err, "Error sending replicate [prevIndex=%v,term=%v,num=%v]", logIndex, logTerm, len(batch))
	// }
//
	// return readResponseResponse(resp)
// }

func (c *client) Replicate(id uuid.UUID, term int, logIndex int, logTerm int, batch []LogItem, commit int) (response, error) {
	resp, err := c.raw.Send(replicateRequest{id, term, batch, logIndex, logTerm, commit}.Request())
	if err != nil {
		return response{}, errors.Wrapf(err, "Error sending replicate [prevIndex=%v,term=%v,num=%v]", logIndex, logTerm, len(batch))
	}

	return readResponseResponse(resp)
}

func (c *client) Append(e Event, source uuid.UUID, seq int, kind int) (LogItem, error) {
	resp, err := c.raw.Send(appendEventRequest{e, source, seq, kind}.Request())
	if err != nil {
		return LogItem{}, errors.Wrapf(err, "Error sending client append: %v", e)
	}

	res, err := readNetAppendEventResponse(resp)
	if err != nil {
		return LogItem{}, errors.Wrapf(err, "Error receiving append response: %v", e)
	}

	return LogItem{res.index, e, res.term, source, seq, kind}, nil
}

func (c *client) RequestVote(id uuid.UUID, term int, logIndex int, logTerm int) (response, error) {
	resp, err := c.raw.Send(requestVoteRequest{id, term, logIndex, logTerm}.Request())
	if err != nil {
		return response{}, errors.Wrapf(err, "Error sending request vote")
	}

	return readResponseResponse(resp)
}

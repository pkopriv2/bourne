package kayak

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

type Client struct {
	peers []string
}

type rpcClient struct {
	raw net.Client
}

func newClient(raw net.Client) *rpcClient {
	return &rpcClient{raw}
}

func (c *rpcClient) Close() error {
	return c.raw.Close()
}

func (c *rpcClient) Status() (status, error) {
	resp, err := c.raw.Send(newStatusRequest())
	if err != nil {
		return status{}, err
	}

	if err := resp.Error(); err != nil {
		return status{}, err
	}

	return readStatusResponse(resp.Body())
}

func (c *rpcClient) UpdateRoster(peer peer, join bool) error {
	resp, err := c.raw.Send(rosterUpdate{peer, join}.Request())
	if err != nil {
		return err
	}

	return resp.Error()
}

func (c *rpcClient) Replicate(r replicateEvents) (response, error) {
	resp, err := c.raw.Send(r.Request())
	if err != nil {
		return response{}, errors.Wrapf(err, "Error sending replicate events: %v", r)
	}

	if err := resp.Error(); err != nil {
		return response{}, err
	}

	return readResponse(resp.Body())
}

func (c *rpcClient) Append(e Event, source uuid.UUID, seq int, kind int) (LogItem, error) {
	return LogItem{}, nil
	// resp, err := c.raw.Send(appendEventRequest{e, source, seq, kind}.Request())
	// if err != nil {
	// return LogItem{}, err
	// }
	//
	// res, err := readNetAppendEventResponse(resp)
	// if err != nil {
	// return LogItem{}, errors.Wrapf(err, "Error receiving append response: %v", e)
	// }
	//
	// return LogItem{res.index, e, res.term, source, seq, kind}, nil
}

func (c *rpcClient) RequestVote(id uuid.UUID, term int, logIndex int, logTerm int) (response, error) {
	return response{}, nil
	// resp, err := c.raw.Send(requestVoteRequest{id, term, logIndex, logTerm}.Request())
	// if err != nil {
	// return response{}, errors.Wrapf(err, "Error sending request vote")
	// }
	//
	// return readResponseResponse(resp)
}

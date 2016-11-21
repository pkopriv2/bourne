package convoy

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
)

// server endpoints
const (
	epDirApply = "/dir/apply"
	epDirList  = "/dir/list"
)

// Meta messages
var (
	metaDirApply = newMeta(epDirApply)
	metaDirList  = newMeta(epDirList)
)

// Meta helpers
func newMeta(action string) scribe.Message {
	return scribe.Build(func(w scribe.Writer) {
		w.Write("action", action)
	})
}

func readMeta(meta scribe.Reader) (string, error) {
	var action string
	return action, meta.Read("action", &action)
}

// some common errors
var (
	MissingMetaError = errors.New("ERROR:Request missing meta")
	MissingBodyError = errors.New("ERROR:Request missing body")
)

// Request/Response helpers

func newDirListRequest() net.Request {
	return net.NewRequest(metaDirList, scribe.EmptyMessage)
}

func newDirListResponse(events []event) net.Response {
	return net.NewStandardResponse(
		scribe.Build(
			func(w scribe.Writer) {
				w.Write("events", events)
			}))
}

func newDirApplyRequest(events []event) net.Request {
	return net.NewRequest(metaDirApply, scribe.Build(func(w scribe.Writer) {
		w.Write("events", events)
	}))
}

func readDirApplyRequest(req net.Request) ([]event, error) {
	var msgs []scribe.Message
	if err := req.Body().Read("events", &msgs); err != nil {
		return nil, errors.Wrap(err, "Error parsing events")
	}

	events := make([]event, 0, len(msgs))
	for _, msg := range msgs {
		e, err := readEvent(msg)
		if err != nil {
			return nil, errors.Wrap(err, "Parsing event requests")
		}

		events = append(events, e)
	}

	return events, nil
}

func newDirApplyResponse(success []bool) net.Response {
	return net.NewStandardResponse(
		scribe.Build(
			func(w scribe.Writer) {
				w.Write("success", success)
			}))
}


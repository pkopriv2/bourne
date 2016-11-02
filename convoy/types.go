package convoy

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/enc"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// I bounced back on forth whether having a "compile time"
var MissingMetaError = errors.New("ERROR:Request missing meta")
var MissingBodyError = errors.New("ERROR:Request missing body")

// Encoding the action within the 'meta' field of a request.
func newMeta(action string) enc.Message {
	return enc.Build(func(w enc.Writer) {
		w.Write("action", action)
	})
}

func readMeta(meta enc.Reader) (string, error) {
	var action string
	return action, meta.Read("action", &action)
}

const (
	rosterAction    = "roster"
	pingAction      = "ping"
	pingProxyAction = "pingProxy"
	updateAction    = "update"
)

var (
	rosterMeta    = newMeta(rosterAction)
	pingMeta      = newMeta(pingAction)
	pingProxyMeta = newMeta(pingProxyAction)
	updateMeta    = newMeta(updateAction)
)

// Ping is special in that its meaning is encodable as a binary
// value.  That means we can use just the existence or
// non-existence of such items as proof of their meaning.
func newPingRequest() net.Request {
	return net.NewEmptyRequest(pingMeta)
}

func newPingResponse() net.Response {
	return net.NewEmptyResponse()
}

// Ping proxies are from the "ping" component of the SWIM protocol.
// If a member detects that another member has failed, they ask
// other members to also ping that member
func newPingProxyRequest(target uuid.UUID) net.Request {
	return net.NewRequest(pingProxyMeta, enc.Build(func(w enc.Writer) {
		w.Write("target", target.String())
	}))
}

func readPingProxyRequest(req net.Request) (uuid.UUID, error) {
	return readUUID(req.Body(), "target")
}

func newPingProxyResponse(success bool) net.Response {
	return net.NewStandardResponse(enc.Build(func(w enc.Writer) {
		w.Write("success", success)
	}))
}

func readPingProxyResponse(r net.Response) (bool, error) {
	var success bool
	return success, r.Body().Read("success", &success)
}

// // Updates are the "transactions" of the roster and are the
// // funamental data unit of dissemination.
// func newUpdateRequest(updates []update) net.Request {
// net.NewRequest(udpateMeta, enc.Build(func(w enc.Writer) {
// w.Write("updates", updates)
// }))
// }
//
// func readUpdateRequest(ctx common.Context, req net.Request) ([]update, error) {
// var msgs []enc.Message
// if err := req.Body().Read("updates", &msgs); err != nil {
// return nil, errors.Wrap(err, "Error parsing updates")
// }
//
// updates := make([]update, 0, len(msgs))
// for _, msg := range msgs {
// u, err := parseUpdate(ctx, msg)
// if err != nil {
// return nil, errors.Wrap(err, "Parsing update requests")
// }
//
// updates = append(updates, u)
// }
//
// return updates, nil
// }
//
// func newUpdateResponseBody(success []bool) net.Message {
// return enc.Build(func(w enc.Writer) {
// w.Write("success", success)
// })
// }
//
// func readUpdateResponseBody(body enc.Reader) (success []bool, err error) {
// return success, body.Read("success", &success)
// }
//
func writeUUID(w enc.Writer, field string, val uuid.UUID) {
	w.Write(field, val.String())
}

func readUUID(r enc.Reader, field string) (uuid.UUID, error) {
	var value string
	if err := r.Read(field, &value); err != nil {
		return *new(uuid.UUID), err
	}

	return uuid.FromString(value)
}

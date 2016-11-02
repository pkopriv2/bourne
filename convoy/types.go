package convoy

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/enc"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// I bounced back on forth whether having a "compile time"
var MissingMetaError = errors.New("ERROR:Request missing meta")
var MissingBodyError = errors.New("ERROR:Request missing body")

const (
	pingResource      string = "ping"
	pingProxyResource string = "pingProxy"
	updateResource    string = "update"
)

// Encoding the resource within the 'meta' field of a request.
func newRequestResource(resource string) enc.Message {
	msg := enc.Build(func(w enc.Writer) {
		w.Write("resource", resource)
	})
	return msg
}

func readRequestResource(r net.Request) (string, error) {
	var ret string

	meta := r.Meta()
	if meta != nil {
		return ret, errors.Wrap(MissingMetaError, "Unable to parse resource")
	}

	if err := meta.Read("resource", &ret); err != nil {
		return ret, errors.Wrap(err, "Unable to parse resource")
	}

	return ret, nil
}

// Ping is special in that its meaning is encodable as a binary
// value.  That means we can use just the existence or
// non-existence of such items as proof of their meaning.
func newPingRequest() net.Request {
	return net.NewEmptyRequest(newRequestResource(pingResource))
}

func newPingResponse() net.Response {
	return net.NewEmptyResponse()
}

// Ping proxies are from the "ping" component of the SWIM protocol.
// If a member detects that another member has failed, they ask
// other members to also ping that member
func newPingProxyRequest(target uuid.UUID) net.Request {
	msg := enc.Build(func(w enc.Writer) {
		w.Write("target", target.String())
	})

	return net.NewRequest(newRequestResource(pingProxyResource), msg)
}

func readPingProxyRequest(req net.Request) (uuid.UUID, error) {
	var ret uuid.UUID

	body := req.Body()
	if body == nil {
		return ret, errors.Wrap(MissingBodyError, "Expected body for ping proxy request")
	}

	return readPingProxyRequestBody(body)
}

func readPingProxyRequestBody(r enc.Reader) (uuid.UUID, error) {
	return readUUID(r, "target")
}

func newPingProxyResponse(success bool) net.Response {
	msg := enc.Build(func(w enc.Writer) {
		w.Write("success", success)
	})

	return net.NewStandardResponse(msg)
}

func readPingProxyResponse(r net.Response) (bool, error) {
	body := r.Body()
	if body == nil {
		return false, errors.Wrap(MissingBodyError, "Expected body for ping proxy response")
	}

	return readPingProxyResponseBody(body)
}

func readPingProxyResponseBody(r enc.Reader) (bool, error) {
	var success bool
	return success, r.Read("success", &success)
}

// Updates are the "transactions" of the roster and are the
// funamental data unit of dissemination.
func newUpdateRequest(updates []update) net.Request {
	msg := enc.Build(func(w enc.Writer) {
		w.Write("updates", updates)
	})

	return net.NewRequest(newRequestResource(updateResource), msg)
}

func readUpdateRequest(ctx common.Context, r net.Request) ([]update, error) {
	body := r.Body()
	if body == nil {
		return nil, errors.Wrap(MissingBodyError, "Expected body for update request")
	}

	var msgs []enc.Message
	if err := body.Read("updates", &msgs); err != nil {
		return nil, errors.Wrap(err, "Error parsing updates")
	}

	updates := make([]update, 0, len(msgs))
	for _, msg := range msgs {
		u, err := parseUpdate(ctx, msg)
		if err != nil {
			return nil, errors.Wrap(err, "Parsing update requests")
		}

		updates = append(updates, u)
	}

	return updates, nil
}

func newUpdateResponse(success []bool) net.Response {
	msg := enc.Build(func(w enc.Writer) {
		w.Write("success", success)
	})

	return net.NewStandardResponse(msg)
}

func readUpdateResponse(r net.Response) (success []bool, err error) {
	body := r.Body()
	if body == nil {
		return nil, errors.Wrap(MissingBodyError, "Expected body for update response")
	}

	return success, body.Read("success", &success)
}

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

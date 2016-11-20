package convoy

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
)

func newReplicaHandler(directory *directory) net.Handler {
	return func(req net.Request) net.Response {
		action, err := readMeta(req.Meta())
		if err != nil {
			return net.NewErrorResponse(errors.Wrap(err, "Error parsing action"))
		}

		switch action {
		default:
			return net.NewErrorResponse(errors.Errorf("Unknown action %v", action))
		case epDirApply:
			return net.NewEmptyResponse()
		case epDirList:
			return net.NewEmptyResponse()
		}
		return nil
	}
}

func handleDirList(dir *directory, req net.Request) net.Response {
	events := dir.Events()
	return net.NewStandardResponse(
		scribe.Build(func(w scribe.Writer) {
			w.Write("events", events)
		}))
}

func handleDirEventApply(dir *directory, req net.Request) net.Response {
	events, err  := readDirApplyRequest(req)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return net.NewStandardResponse(newDirApplyResponseBody(dir.ApplyAll(events)))
}

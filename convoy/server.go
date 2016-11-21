package convoy

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/net"
)

func newReplicaHandler(dir *directory) net.Handler {
	return func(req net.Request) net.Response {
		action, err := readMeta(req.Meta())
		if err != nil {
			return net.NewErrorResponse(errors.Wrap(err, "Error parsing action"))
		}

		switch action {
		default:
			return net.NewErrorResponse(errors.Errorf("Unknown action %v", action))
		case epDirApply:
			return handleDirList(dir, req)
		case epDirList:
			return handleDirApply(dir, req)
		}
	}
}

func handleDirList(dir *directory, req net.Request) net.Response {
	return newDirListResponse(dir.Events())
}

func handleDirApply(dir *directory, req net.Request) net.Response {
	events, err := readDirApplyRequest(req)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return newDirApplyResponse(dir.ApplyAll(events))
}

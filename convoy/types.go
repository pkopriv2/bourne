package convoy

import (
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

const (
	pingType      = 1
	pingProxyType = 2
	updateType    = 3
)

func newPingRequest() net.Request {
	return net.NewRequest(pingType, nil)
}

func newPingProxyRequest(target uuid.UUID) net.Request {
	return net.NewRequest(pingProxyType, body)
}

func newUpdateRequest(updates []update) net.Request {
	// return net.NewRequest(updateType, updates)
	return net.NewRequest(0, nil)
}

func newPingResponse() net.Response {
	// return net.NewSuccessResponse(true)
	return net.NewSuccessResponse(nil)
}

func newPingProxyResponse(success bool) net.Response {
	// return net.NewSuccessResponse(success)
	return net.NewSuccessResponse(nil)
}

func newUpdateResponse(success []bool) net.Response {
	// return net.NewSuccessResponse(success)
	return net.NewSuccessResponse(nil)
}

func parsePingProxyResponse(resp net.Response) (bool, error) {
	return false, nil
	// if err := resp.Error; err != nil {
		// return false, err
	// }
//
	// return resp.Body.(bool), nil
}

func parseUpdateResponse(resp net.Response) ([]bool, error) {
	return nil, nil
	// if err := resp.Error; err != nil {
		// return nil, err
	// }
//
	// return resp.Body.([]bool), nil
}

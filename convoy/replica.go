package convoy

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
)

var PeerClosedError = errors.New("ERROR:PEER:CLOSED")

type replica struct {
	ctx common.Context
	// directory *directory
	server net.Server

	closer chan struct{}
	closed chan struct{}
	wait   sync.WaitGroup
}

func (p *replica) Close() error {
	select {
	case <-p.closed:
		return PeerClosedError
	case p.closer <- struct{}{}:
	}

	close(p.closed)
	p.wait.Wait()
	return nil
}

// func newDirectoryHandler(directory *directory) net.Handler {
// return func(req net.Request) net.Response {
// action, err := readMeta(req.Meta())
// if err != nil {
// return net.NewErrorResponse(errors.Wrap(err, "Error parsing action"))
// }
//
// switch action {
// default:
// return net.NewErrorResponse(errors.Errorf("Unknown action %v", action))
// case pingAction:
// return net.NewEmptyResponse()
// }
// return nil
// }
// }

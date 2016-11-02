package convoy

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
)

var PeerClosedError = errors.New("ERROR:PEER:CLOSED")

func StartCluster(ctx common.Context, port int) (Peer, error) {
	peer := &peer{
		ctx:    ctx,
		roster: newRoster(),
		closer: make(chan struct{}),
		closed: make(chan struct{}),
	}

	if err := peer.startServer(); err != nil {
		return nil, err
	}

	return peer, nil
}

type peer struct {
	ctx    common.Context
	roster Roster
	port   int

	closer chan struct{}
	closed chan struct{}
	wait   sync.WaitGroup
}

func (p *peer) Close() error {
	select {
	case <-p.closed:
		return PeerClosedError
	case p.closer <- struct{}{}:
	}

	close(p.closed)
	p.wait.Wait()
	return nil
}

func (p *peer) client() (client, error) {
	client, err := net.NewTcpClient(p.ctx, net.NewAddr("localhost", p.port))
	if err != nil {
		return nil, err
	}

	return newClient(client), nil
}

func (p *peer) Roster() Roster {
	return p.roster
}

// func (p *peer) ping(target uuid.UUID) (bool, error) {
// member :=  p.roster.Get(target)
// if member == nil {
// return false, nil
// }
//
// client, err := member.client()
// if err != nil {
// return false, nil
// }
// defer client.Close()
// return client.Ping()
// }
//
// func (p *peer) update(updates []update) ([]bool, error) {
// ret := make([]bool, 0, len(updates))
// for _, u := range updates {
// ret = append(ret, u.Apply(p.Roster()))
// }
// return ret, nil
// }

func (p *peer) startServer() error {
	server, err := net.NewTcpServer(p.ctx, 0, newPeerHandler(p))
	if err != nil {
		return err
	}

	go func() {
		defer server.Close()
		<-p.closed
	}()
	return nil
}

func newPeerHandler(peer *peer) net.Handler {
	return func(req net.Request) net.Response {
		action, err := readMeta(req.Meta())
		if err != nil {
			return net.NewErrorResponse(errors.Wrap(err, "Error parsing action"))
		}

		switch action {
		default:
			return net.NewErrorResponse(errors.Errorf("Unknown action %v", action))
		case pingAction:
			return net.NewEmptyResponse()
		}
		return nil
	}
}

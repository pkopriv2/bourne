package convoy

import (
	"sync"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	uuid "github.com/satori/go.uuid"
)

func StartPeer(ctx common.Context, clusterHost string, clusterPort int) (Peer, error) {
	return nil, nil
}

type peer struct {
	ctx    common.Context
	roster Roster
	diss   Disseminator
	pool   concurrent.WorkPool

	closer chan struct{}
	closed chan struct{}
	wait   sync.WaitGroup
	fail   concurrent.Val
}

func (p *peer) Close() error {
	panic("not implemented")
}

func (p *peer) Roster() Roster {
	return p.roster
}

func (p *peer) ping(uuid.UUID) (bool, error) {
	panic("not implemented")
}

func (p *peer) update([]update) ([]bool, error) {
	panic("not implemented")
}

// func servePeer(peer *peer) {
// defer peer.wait.Done()
//
// listener, err := net.ListenTCP(0)
// if err != nil {
// return
// }
//
// pool := concurrent.NewWorkPool(
// peer.ctx.Config().OptionalInt(
// confServerPoolSize, defaultServerPoolSize))
//
// newServer(pool, listener, newPeerHandler(peer))
// }
//
// func newPeerHandler(peer Peer) handler {
// return func(req request) response {
// switch req.Type {
// default:
// return newErrorResponse(newUnknownRequestError(req.Type))
// case pingType:
// return newPingResponse()
// case pingProxyType:
// return handlePingProxy(peer, req.Body.(uuid.UUID))
// case updateType:
// return handleUpdate(peer, req.Body.([]update))
// }
// }
// }
//
// func handlePingProxy(peer Peer, target uuid.UUID) response {
// success, err := peer.ping(target)
// if err != nil {
// return newErrorResponse(err)
// }
//
// return newPingProxyResponse(success)
// }
//
// func handleUpdate(peer Peer, updates []update) response {
// success, err := peer.update(updates)
// if err != nil {
// return newErrorResponse(err)
// }
//
// return newUpdateResponse(success)
// }

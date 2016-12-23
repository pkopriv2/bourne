package kayak

import (
	"fmt"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	uuid "github.com/satori/go.uuid"
)

// References:
//
// * https://raft.github.io/raft.pdf
// * https://www.youtube.com/watch?v=LAqyTyNUYSY
// * https://github.com/ongardie/dissertation/blob/master/book.pdf?raw=true
//
//
// Considered a BFA (Byzantine-Federated-Agreement) approach, but looked too complex for our
// initial needs. (consider for future systems)
//
// * https://www.stellar.org/papers/stellar-consensus-protocol.pdf
//
// NOTE: Currently only election is implemented.
// TODO:
//  * Support proper client appends + log impl
//  * Support changing cluster membership
//  * Support durable log!!
//

func hostsCollect(hosts []*host, fn func(h *host) bool) []*host {
	ret := make([]*host, 0, len(hosts))
	for _, h := range hosts {
		if fn(h) {
			ret = append(ret, h)
		}
	}
	return ret
}

func hostsFirst(hosts []*host, fn func(h *host) bool) *host {
	for _, h := range hosts {
		if fn(h) {
			return h
		}
	}
	return nil
}

// A peer contains the identifying info of a cluster member.
type peer struct {
	id   uuid.UUID
	addr string
}

func (p peer) String() string {
	return fmt.Sprintf("Peer(%v, %v)", p.id.String()[:8], p.addr)
}

func (p peer) Client(ctx common.Context) (*client, error) {
	raw, err := net.NewTcpClient(ctx, ctx.Logger(), p.addr)
	if err != nil {
		return nil, err
	}

	return &client{raw}, nil
}

type host struct {
	ctx    common.Context
	logger common.Logger
	member *member
	server net.Server
	closed chan struct{}
	closer chan struct{}
}

func newHost(ctx common.Context, self peer, others []peer) (h *host, err error) {
	root := ctx.Logger().Fmt("Kayak: %v", self)
	root.Info("Starting host with peers [%v]", others)

	member, err := newMember(ctx, root, self, others)
	if err != nil {
		return nil, err
	}
	defer common.RunIf(func() { member.Close() })(err)

	_, port, err := net.SplitAddr(self.addr)
	if err != nil {
		return nil, err
	}

	server, err := newServer(ctx, root, port, member)
	if err != nil {
		return nil, err
	}
	defer common.RunIf(func() { server.Close() })(err) // paranoia of future me
	h = &host{
		member: member,
		server: server,
		logger: root,
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1),
	}
	return
}

func (h *host) Id() uuid.UUID {
	return h.member.instance.id
}

func (h *host) Close() error {
	select {
	case <-h.closed:
		return ClosedError
	case h.closer <- struct{}{}:
	}

	h.logger.Info("Closing")

	var err error
	err = common.Or(err, h.member.Close())
	err = common.Or(err, h.server.Close())

	close(h.closed)
	return err
}

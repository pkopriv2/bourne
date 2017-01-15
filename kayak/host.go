package kayak

import (
	"fmt"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/stash"
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
//  * Support changing cluster membership
//  * Support durable log!!
//

// A peer contains the identifying info of a cluster member.
type peer struct {
	id   uuid.UUID
	addr string
}

func newPeer(addr string) peer {
	return peer{id: uuid.NewV1(), addr: addr}
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
	logger common.Logger
	core   *replicatedLog
	server net.Server
	closed chan struct{}
	closer chan struct{}
}

func newHost(ctx common.Context, self peer, others []peer, stash stash.Stash) (h *host, err error) {
	root := ctx.Logger().Fmt("Kayak: %v", self)
	root.Info("Starting host with peers [%v]", others)

	core, err := newReplicatedLog(ctx, root, self, others, stash)
	if err != nil {
		return nil, err
	}
	defer common.RunIf(func() { core.Close() })(err)

	_, port, err := net.SplitAddr(self.addr)
	if err != nil {
		return nil, err
	}

	server, err := newServer(ctx, root, port, core)
	if err != nil {
		return nil, err
	}
	defer common.RunIf(func() { server.Close() })(err) // paranoia of future me

	h = &host{
		core:   core,
		server: server,
		logger: root,
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1),
	}
	return
}

func (h *host) Id() uuid.UUID {
	return h.core.replica.Id
}

func (h *host) Context() common.Context {
	return h.core.replica.Ctx
}

func (h *host) Self() peer {
	return h.core.Self()
}

func (h *host) Peers() []peer {
	return h.core.Peers()
}

func (h *host) Log() *eventLog {
	return h.core.Log()
}

func (h *host) Append(e Event) (LogItem, error) {
	return h.core.Append(e)
}

func (h *host) Client() (*client, error) {
	return h.core.Self().Client(h.Context())
}

func (h *host) Listen(from int, buf int) (Listener, error) {
	return h.core.Listen(from, buf)
}

func (h *host) Close() error {
	select {
	case <-h.closed:
		return ClosedError
	case h.closer <- struct{}{}:
	}

	h.logger.Info("Closing")

	var err error
	err = common.Or(err, h.server.Close())
	err = common.Or(err, h.core.Close())
	close(h.closed)
	return err
}

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

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

// A peer contains the identifying info of a cluster member.
type peer struct {
	id   uuid.UUID
	addr string
}

func (p peer) String() string {
	return fmt.Sprintf("Peer(%v)", p.id.String()[:8], p.addr)
}

func (p peer) Client(ctx common.Context) (*client, error) {
	raw, err := net.NewTcpClient(ctx, ctx.Logger(), p.addr)
	if err != nil {
		return nil, err
	}

	return &client{raw}, nil
}

type host struct {
	ctx     common.Context
	logger  common.Logger
	member *member
	server  net.Server
	closed  chan struct{}
	closer  chan struct{}
}

func newHost(ctx common.Context, self peer, others []peer) (h *host, err error) {
	root := ctx.Logger().Fmt("Kayak: %v", self)
	root.Info("Starting host with peers [%v]", others)

	// member := &member{
	// ctx:     ctx,
	// id:      self.id,
	// self:    self,
	// peers:   others,
	// log:     newViewLog(ctx),
	// appends: make(chan appendEvents),
	// votes:   make(chan requestVote),
	// timeout: time.Millisecond * time.Duration((rand.Intn(1000) + 1000)),
	// }
	// member, err := newMember(ctx, root, self, others)
	// if err != nil {
	// return nil, err
	// }
	// defer common.RunIf(func() { member.Close() })(err)

	// server, err := newServer(ctx, root, self.port, member)
	// if err != nil {
	// return nil, err
	// }
	// defer common.RunIf(func() { server.Close() })(err)
	//
	// h = &host{
	// member: member,
	// server: server,
	// logger: root,
	// closed: make(chan struct{}),
	// closer: make(chan struct{}, 1),
	// }
	return
}

func (h *host) Close() error {
	select {
	case <-h.closed:
		return ClosedError
	case h.closer <- struct{}{}:
	}

	h.logger.Info("Closing")

	var err error
	// err = common.Or(err, h.member.Close())
	err = common.Or(err, h.server.Close())

	close(h.closed)
	return err
}

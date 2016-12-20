package kayak

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
)

type host struct {
	member *member
	server net.Server
	logger common.Logger

	// closing utilities.
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

	server, err := newServer(ctx, root, self.port, member)
	if err != nil {
		return nil, err
	}
	defer common.RunIf(func() { server.Close() })(err)

	h = &host{
		member: member,
		server: server,
		logger: root,
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1),
	}
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
	err = common.Or(err, h.member.Close())
	err = common.Or(err, h.server.Close())

	close(h.closed)
	return err
}

package elmer

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
)


// a peer simply binds a network service with the core store machine.
type peer struct {
	ctx    common.Context
	ctrl   common.Control
	logger common.Logger
	net    net.Network
	self   kayak.Host
	core   *machine
	server net.Server
}

func newPeer(ctx common.Context, self kayak.Host, net net.Network, addr string) (h *peer, err error) {
	ctx = ctx.Sub("Elmer")
	defer func() {
		if err != nil {
			ctx.Control().Fail(err)
		}
	}()

	listener, err := net.Listen(30*time.Second, addr)
	if err != nil {
		return nil, err
	}
	ctx.Control().Defer(func(cause error) {
		listener.Close()
	})

	core, err := newStoreMachine(ctx, self, 20)
	if err != nil {
		return nil, err
	}
	ctx.Control().Defer(func(cause error) {
		core.Close()
	})

	server, err := newServer(ctx, listener, core, 50)
	if err != nil {
		return nil, err
	}
	ctx.Control().Defer(func(cause error) {
		server.Close()
	})

	return &peer{
		ctx:    ctx,
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
		net:    net,
		core:   core,
		server: server,
	}, nil
}

func (p *peer) Close() error {
	return p.ctrl.Close()
}

func (p *peer) Store() (Store, error) {
	return newStoreClient(p.ctx, p.net, 30*time.Second, 5*time.Minute, p.self.Roster()), nil
}

func (p *peer) Shutdown() error {
	return p.ctrl.Close()
}

type roster struct {
	peers []string
}

func (r roster) Write(w scribe.Writer) {
	w.WriteStrings("peers", r.peers)
}

func (r roster) Bytes() {
	scribe.Write(r).Bytes()
}

func readRoster(r scribe.Reader) (ret roster, err error) {
	err = r.ReadStrings("peers", &ret.peers)
	return
}

func parseRosterBytes(bytes []byte) (ret roster, err error) {
	msg, err := scribe.Parse(bytes)
	if err != nil {
		return roster{}, errors.WithStack(err)
	}

	ret, err = readRoster(msg)
	if err != nil {
		return roster{}, errors.WithStack(err)
	}
	return
}

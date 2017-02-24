package elmer

import (
	"math/rand"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
)

type rosterSync struct {
	ctx     common.Context
	ctrl    common.Control
	logger  common.Logger
	net     net.Network
	timeout time.Duration
	freq    time.Duration
	roster  chan []string
}

func newRosterSync(ctx common.Context, net net.Network, timeout time.Duration, freq time.Duration, base []string) *rosterSync {
	ctx = ctx.Sub("RosterManager")
	r := &rosterSync{
		ctx:     ctx,
		ctrl:    ctx.Control(),
		logger:  ctx.Logger(),
		net:     net,
		timeout: timeout,
		freq:    freq,
		roster:  make(chan []string),
	}
	r.start(base)
	return r
}

func (r *rosterSync) Close() error {
	return r.ctrl.Close()
}

func (r *rosterSync) start(base []string) {
	cur := r.refreshRoster(base)

	go func() {
		defer r.ctrl.Close()

		for !r.ctrl.IsClosed() {
			timer := time.NewTimer(r.freq)
			select {
			case <-r.ctrl.Closed():
				return
			case r.roster <- cur:
				continue
			case <-timer.C:
			}
			cur = r.refreshRoster(cur)
			r.logger.Info("Refreshed roster: %v", cur)
		}
	}()
}

func (r *rosterSync) refreshRoster(prev []string) []string {
	ret := prev
	for _, peer := range rosterShuffle(prev) {
		cl, err := connect(r.ctx, r.net, r.timeout, peer)
		if err != nil {
			r.logger.Error("Error connecting to peer [%v]: %+v", peer, err)
			continue
		}

		stat, err := cl.Status()
		if err != nil {
			r.logger.Error("Error retrieving status from peer [%v]: %+v", peer, err)
			continue
		}

		return stat.peers
	}
	return ret
}

func (r *rosterSync) Roster() ([]string, error) {
	select {
	case <-r.ctrl.Closed():
		return nil, errors.WithStack(common.ClosedError)
	case r := <-r.roster:
		return r, nil
	}
}

func rosterShuffle(all []string) []string {
	ret := make([]string, len(all))
	for i, j := range rand.Perm(len(all)) {
		ret[i] = all[j]
	}
	return ret
}

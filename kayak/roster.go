package kayak

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

type rosterManager struct {
	logger common.Logger
	self   *replica
}

func listenRosterChanges(r *replica) {
	m := &rosterManager{r.Ctx.Logger().Fmt("RosterManager"), r}
	m.start()
}

func (r *rosterManager) start() {
	go func() {
		defer r.logger.Info("Shutting down")

		for {
			r.logger.Info("Rebuilding roster")

			peers, until, err := r.reloadRoster()
			if err != nil {
				r.self.ctrl.Fail(err)
				return
			}

			r.self.Roster.Set(peers)

			appends, err := r.listenAppends(until + 1)
			if err != nil {
				r.self.ctrl.Fail(err)
				return
			}
			defer appends.Close()

			commits, err := r.listenCommits(until + 1)
			if err != nil {
				r.self.ctrl.Fail(err)
				return
			}
			defer commits.Close()

			ctrl := r.self.Ctx.Control().Sub()
			go func() {
				l := newConfigListener(appends, ctrl)

				peers, ok, err := l.Next()
				for ; ok; peers, ok, err = l.Next() {

					r.logger.Info("Updating roster: %v", peers)
					r.self.Roster.Set(peers)
				}

				ctrl.Fail(err)
			}()

			go func() {
				l := newConfigListener(commits, ctrl)

				member := false

				peers, ok, err := l.Next()
				for ; ok; peers, ok, err = l.Next() {
					if hasPeer(peers, r.self.Self) {
						member = true
					}

					if member && !hasPeer(peers, r.self.Self) {
						r.logger.Info("No longer a member of the cluster [%v]", peers)
						r.self.ctrl.Close()
						ctrl.Close()
						return
					}
				}

				ctrl.Fail(err)
			}()

			select {
			case <-r.self.ctrl.Closed():
				return
			case <-ctrl.Closed():
				if cause := extractError(ctrl.Failure()); cause != OutOfBoundsError {
					r.self.ctrl.Fail(err)
					return
				}
			}
		}
	}()
}

func (r *rosterManager) reloadRoster() ([]peer, int, error) {
	snapshot, err := r.self.Log.Snapshot()
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Error getting snapshot")
	}

	peers, err := parsePeers(snapshot.Config())
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Error parsing config")
	}

	return peers, snapshot.LastIndex(), nil
}

func (r *rosterManager) listenAppends(offset int) (Listener, error) {
	r.logger.Info("Listening to appends from offset [%v]", offset)

	l, err := r.self.Log.ListenAppends(offset, 256)
	if err != nil {
		return nil, errors.Wrapf(err, "Error registering listener at offset [%v]", offset)
	}

	return l, nil
}

func (r *rosterManager) listenCommits(offset int) (Listener, error) {
	r.logger.Info("Listening to commits from offset [%v]", offset)

	l, err := r.self.Log.ListenCommits(offset, 256)
	if err != nil {
		return nil, errors.Wrapf(err, "Error registering listener at offset [%v]", offset)
	}

	return l, nil
}

type roster struct {
	raw []peer
	ver *ref
}

func newRoster(init []peer) *roster {
	return &roster{raw: init, ver: newRef(0)}
}

func (c *roster) Wait(next int) ([]peer, int, bool) {
	_, ok := c.ver.WaitExceeds(next)
	peers, ver := c.Get()
	return peers, ver, ok
}

func (c *roster) Notify() {
	c.ver.Notify()
}

func (c *roster) Set(peers []peer) {
	c.ver.Update(func(cur int) int {
		c.raw = peers
		return cur + 1
	})
}

// not taking copy as it is assumed that array is immutable
func (c *roster) Get() (peers []peer, ver int) {
	c.ver.Update(func(cur int) int {
		peers, ver = c.raw, cur
		return cur
	})
	return
}

func (c *roster) Close() {
	c.ver.Close()
}

type configListener struct {
	raw  Listener
	ctrl common.Control
}

func newConfigListener(raw Listener, ctrl common.Control) *configListener {
	return &configListener{raw, ctrl}
}

func (p *configListener) Next() ([]peer, bool, error) {
	for {
		if p.ctrl.IsClosed() {
			return nil, false, p.ctrl.Failure()
		}

		next, ok, err := p.raw.Next()
		if err != nil || !ok {
			return nil, false, err
		}

		if next.Kind != Config {
			continue
		}

		peers, err := parsePeers(next.Event)
		if err != nil {
			return nil, false, err
		}

		return peers, true, nil
	}
}

func (l *configListener) Close() error {
	return l.raw.Close()
}

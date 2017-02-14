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

			ctrl := r.self.ctrl.Sub()
			go func() {
				defer ctrl.Close()

				l := newConfigListener(appends, ctrl)
				for {
					var peers []Peer
					select {
					case <-ctrl.Closed():
						return
					case <-l.Ctrl().Closed():
						return
					case peers = <-l.Data():
					}

					r.logger.Info("Updating roster: %v", peers)
					r.self.Roster.Set(peers)
				}
			}()

			go func() {
				defer ctrl.Close()

				l := newConfigListener(commits, ctrl)

				// FIXME: Must play log out to make sure we aren't re-added!
				for member := false; ; {
					var peers []Peer
					select {
					case <-ctrl.Closed():
						return
					case <-l.Ctrl().Closed():
						return
					case peers = <-l.Data():
					}

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
			}()

			select {
			case <-r.self.ctrl.Closed():
				return
			case <-ctrl.Closed():
				if cause := common.Extract(ctrl.Failure(), OutOfBoundsError); cause != OutOfBoundsError {
					r.self.ctrl.Fail(err)
					return
				}
			}
		}
	}()
}

func (r *rosterManager) reloadRoster() ([]Peer, int, error) {
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
	raw []Peer
	ver *ref
}

func newRoster(init []Peer) *roster {
	return &roster{raw: init, ver: newRef(0)}
}

func (c *roster) Wait(next int) ([]Peer, int, bool) {
	_, ok := c.ver.WaitExceeds(next)
	peers, ver := c.Get()
	return peers, ver, ok
}

func (c *roster) Notify() {
	c.ver.Notify()
}

func (c *roster) Set(peers []Peer) {
	c.ver.Update(func(cur int) int {
		c.raw = peers
		return cur + 1
	})
}

// not taking copy as it is assumed that array is immutable
func (c *roster) Get() (peers []Peer, ver int) {
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
	ch   chan []Peer
}

func newConfigListener(raw Listener, ctrl common.Control) *configListener {
	l := &configListener{raw, ctrl, make(chan []Peer)}
	l.start()
	return l
}

func (c *configListener) Close() error {
	return c.ctrl.Close()
}

func (c *configListener) Ctrl() common.Control {
	return c.ctrl
}

func (c *configListener) Data() <-chan []Peer {
	return c.ch
}

func (p *configListener) start() {
	go func() {
		for {
			var next Entry
			select {
			case <-p.ctrl.Closed():
				return
			case <-p.raw.Ctrl().Closed():
				p.ctrl.Fail(errors.WithStack(p.raw.Ctrl().Failure()))
				return
			case next = <-p.raw.Data():
			}

			if next.kind != Conf {
				continue
			}

			peers, err := parsePeers(next.Event)
			if err != nil {
				p.ctrl.Fail(errors.WithStack(err))
				return
			}

			select {
			case <-p.ctrl.Closed():
				return
			case p.ch <- peers:
			}
		}
	}()
}

package kayak

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

type logClient struct {
	ctx    common.Context
	ctrl   common.Control
	logger common.Logger

	// the id of the logClient.
	id uuid.UUID

	// the core replica instance.  (used mostly for)
	self *replica

	// the raw leader client
	pool common.ObjectPool
}

func newLogClient(self *replica, pool common.ObjectPool) *logClient {
	ctx := self.Ctx.Sub("LogClient")
	return &logClient{
		ctx:    ctx,
		logger: ctx.Logger(),
		ctrl:   ctx.Control(),
		self:   self,
		pool:   pool,
	}
}

func (s *logClient) Id() uuid.UUID {
	return s.id
}

func (c *logClient) Close() error {
	return c.ctrl.Close()
}

func (c *logClient) Append(cancel <-chan struct{}, e Event) (entry Entry, err error) {
	raw := c.pool.TakeOrCancel(cancel)
	if raw == nil {
		return Entry{}, errors.WithStack(CanceledError)
	}
	defer func() {
		if err != nil {
			c.pool.Fail(raw)
		} else {
			c.pool.Return(raw)
		}
	}()
	resp, err := raw.(*rpcClient).Append(appendEvent{e, Std})
	if err != nil {
		return Entry{}, err
	}

	return Entry{resp.index, e, resp.term, Std}, nil
}

func (s *logClient) Listen(start int, buf int) (Listener, error) {
	raw, err := s.self.Log.ListenCommits(start, buf)
	if err != nil {
		return nil, err
	}
	return newLogClientListener(raw), nil
}

func (s *logClient) Snapshot() (int, EventStream, error) {
	snapshot, err := s.self.Log.Snapshot()
	if err != nil {
		return 0, nil, err
	}

	return snapshot.LastIndex(), newSnapshotStream(s.ctrl, snapshot, 1024), nil
}

func (s *logClient) Compact(until int, data <-chan Event, size int) error {
	return s.self.Compact(until, data, size)
}

type logClientListener struct {
	raw Listener
	dat chan Entry
}

func newLogClientListener(raw Listener) *logClientListener {
	return &logClientListener{raw, make(chan Entry, 128)}
}

func (p *logClientListener) start() {
	go func() {
		for {
			var e Entry
			select {
			case <-p.Ctrl().Closed():
				return
			case e = <-p.raw.Data():
			}

			if e.Kind != Std {
				continue
			}

			select {
			case <-p.Ctrl().Closed():
				return
			case p.dat<-e:
			}
		}
	}()
}

func (l *logClientListener) Close() error {
	return l.raw.Close()
}

func (l *logClientListener) Ctrl() common.Control {
	return l.raw.Ctrl()
}

func (l *logClientListener) Data() <-chan Entry {
	return l.dat
}


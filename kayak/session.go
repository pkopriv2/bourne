package kayak

import (
	"time"

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

func (c *logClient) Append(timeout time.Duration, e Event) (entry Entry, err error) {
	raw := c.pool.TakeTimeout(timeout)
	if raw == nil {
		return Entry{}, errors.Wrapf(TimeoutError, "Unable to append. Timeout [%v] while waiting for client.", timeout)
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

func (s *logClient) Snapshot() (EventStream, error) {
	snapshot, err := s.self.Log.Snapshot()
	if err != nil {
		return nil, err
	}

	return newSnapshotStream(s.ctrl, snapshot, 1024), nil
}

func (s *logClient) Compact(until int, data <-chan Event, size int) error {
	return s.self.Compact(until, data, size)
}

type logClientListener struct {
	raw Listener
}

func newLogClientListener(raw Listener) *logClientListener {
	return &logClientListener{raw}
}

func (p *logClientListener) Next() (Entry, bool, error) {
	for {
		next, ok, err := p.raw.Next()
		if err != nil || !ok {
			return next, ok, err
		}

		if next.Kind == Std {
			return next, true, nil
		}
	}
}

func (l *logClientListener) Close() error {
	return l.raw.Close()
}

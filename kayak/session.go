package kayak

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

func sessionExpiredError(id uuid.UUID, timeout time.Duration) error {
	return errors.Wrapf(ExpiredError, "Session [%v] expired [%v]", id, timeout)
}

type sessionAppendRequest struct {
	Event   Event
	Kind    Kind
	Timeout time.Duration
}

type sessionTick struct {
	Id  uuid.UUID
	Ttl int
}

func (s sessionTick) Write(w scribe.Writer) {
	w.WriteUUID("id", s.Id)
	w.WriteInt("ttl", s.Ttl)
}

func readSessionTick(r scribe.Reader) (evt sessionTick, err error) {
	err = common.Or(err, r.ReadUUID("id", &evt.Id))
	err = common.Or(err, r.ReadInt("ttl", &evt.Ttl))
	return
}

type session struct {
	ctx    common.Context
	ctrl   common.Control
	logger common.Logger

	// the id of the session.
	id uuid.UUID

	// the core replica instance.  (used mostly for)
	self *replica

	// the expiration
	exp time.Duration

	// the index of the last successful append.
	last time.Time

	// request channel
	req chan *common.Request

	// the raw leader client
	pool common.ObjectPool
}

func newSession(self *replica, pool common.ObjectPool, id uuid.UUID) (*session, error) {
	ctx := self.Ctx.Sub("Session(%v)", id.String()[:8])

	s := &session{
		ctx:    ctx,
		logger: ctx.Logger(),
		ctrl:   ctx.Control(),
		id:     id,
		self:   self,
		last:   time.Now(),
		exp:    5 * time.Minute,
		req:    make(chan *common.Request),
		pool:   pool,
	}

	if err := s.start(); err != nil {
		return nil, err
	}

	if err := s.sessionTick(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *session) Id() uuid.UUID {
	return s.id
}

func (c *session) Close() error {
	return c.ctrl.Close()
}

func (c *session) Append(timeout time.Duration, e Event) (Entry, error) {
	return c.append(timeout, e, Std)
}

func (s *session) Listen(start int, buf int) (Listener, error) {
	raw, err := s.self.Log.ListenCommits(start, buf)
	if err != nil {
		return nil, err
	}
	return newSessionListener(raw), nil
}

func (s *session) Snapshot() (EventStream, error) {
	snapshot, err := s.self.Log.Snapshot()
	if err != nil {
		return nil, err
	}

	return newSnapshotStream(s.ctrl, snapshot, 1024), nil
}

func (s *session) Compact(until int, data <-chan Event, size int) error {
	return s.self.Compact(until, data, size)
}

func (c *session) sessionExpired() bool {
	return time.Now().Sub(c.last) > c.exp
}

func (c *session) sessionTick() error {
	_, err := c.append(c.self.RequestTimeout, scribe.Write(sessionTick{c.id, int(c.exp)}).Bytes(), Tick)
	return err
}

func (c *session) append(timeout time.Duration, e Event, k Kind) (Entry, error) {
	req := common.NewRequest(sessionAppendRequest{e, k, timeout})

	timer := time.NewTimer(timeout)
	select {
	case <-c.ctrl.Closed():
		return Entry{}, ClosedError
	case <-timer.C:
		return Entry{}, errors.Wrapf(TimeoutError, "Timeout [%v] append event for session [%v]", timeout, c.id)
	case c.req <- req:
		select {
		case <-c.ctrl.Closed():
			return Entry{}, ClosedError
		case err := <-req.Failed():
			return Entry{}, err
		case val := <-req.Acked():
			return val.(Entry), nil
		}
	}
}

func (c *session) start() error {
	go func() {
		timer := time.NewTimer(c.exp / 2)
		for {
			select {
			case <-c.ctrl.Closed():
				return
			case <-timer.C:
				if err := c.sessionTick(); err != nil {
					c.ctrl.Fail(err)
					return
				}
			}
		}
	}()

	go func() {
		for seq := 0; ; seq++ {
			timer := time.NewTimer(c.exp / 2)

			var req *common.Request
			select {
			case <-c.ctrl.Closed():
				return
			case <-timer.C:
				if c.sessionExpired() {
					c.ctrl.Fail(sessionExpiredError(c.id, c.exp))
					return
				}
				continue
			case req = <-c.req:
			}

			append := req.Body().(sessionAppendRequest)

			for atmpt := 0; ; atmpt++ {
				c.logger.Debug("Attempt [%v] appending event [%v]", atmpt, seq)

				if c.ctrl.IsClosed() {
					req.Fail(ClosedError)
					break
				}

				raw := c.pool.TakeTimeout(append.Timeout)
				if raw == nil {
					req.Fail(errors.Wrapf(TimeoutError, "Timeout [%v] append event for session [%v]", append.Timeout, c.id))
					break
				}

				cl := raw.(*rpcClient)

				entry, err := c.tryAppend(cl, seq, append.Event, append.Kind)
				if err != nil {
					cl.Close()
					c.pool.Fail(cl)
					continue
				}

				c.pool.Return(cl)

				now := time.Now()
				if now.Sub(c.last) > c.exp {
					err := sessionExpiredError(c.id, c.exp)
					c.ctrl.Fail(err)
					req.Fail(err)
					return
				}

				c.last = now
				req.Ack(entry)
				break
			}
		}
	}()

	return nil
}

func (c *session) tryAppend(cl *rpcClient, seq int, evt Event, kind Kind) (Entry, error) {
	resp, err := cl.Append(appendEvent{evt, c.id, seq, kind})
	if err != nil {
		return Entry{}, err
	}

	return Entry{resp.index, evt, resp.term, c.id, seq, kind}, nil
}

type sessionListener struct {
	raw Listener
	seq map[uuid.UUID]int
}

func newSessionListener(raw Listener) *sessionListener {
	return &sessionListener{raw, make(map[uuid.UUID]int)}
}

func (p *sessionListener) Next() (Entry, bool, error) {
	for {
		next, ok, err := p.raw.Next()
		if err != nil || !ok {
			return next, ok, err
		}

		cur := p.seq[next.Session]
		if next.Seq <= cur || next.Seq > 1024 {
			continue
		}

		if next.Seq >= 1024 {
			delete(p.seq, next.Session)
		} else {
			p.seq[next.Session] = next.Seq
		}

		return next, true, nil
	}
}

func (l *sessionListener) Close() error {
	return l.raw.Close()
}

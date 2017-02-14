package elmer

import (
	"bytes"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
)

type machine struct {
	ctx    common.Context
	ctrl   common.Control
	logger common.Logger
	peer   kayak.Host
	addr   string
	read   chan *common.Request
	swap   chan *common.Request
	pool   common.WorkPool
}

func newStoreMachine(ctx common.Context, peer kayak.Host, workers int) (*machine, error) {
	m := &machine{
		ctx:    ctx,
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
		peer:   peer,
		read:   make(chan *common.Request),
		swap:   make(chan *common.Request),
		pool:   common.NewWorkPool(ctx.Control(), workers),
	}

	m.start()
	return m, nil
}

func (m *machine) Close() error {
	return m.ctrl.Close()
}

func (s *machine) start() {
	go func() {
		defer s.ctrl.Close()

		for iter := 0; ; iter++ {
			s.logger.Info("Starting epoch [%v]", iter)

			log, sync, err := s.getLog()
			if err != nil {
				s.logger.Error("Error retrieving log: %+v", err)
				continue
			}

			epoch, err := openEpoch(s.ctx, log, sync, iter)
			if err != nil {
				s.logger.Error("Error retrieving log: %+v", err)
				continue
			}

			for !s.ctrl.IsClosed() {
				select {
				case <-s.ctrl.Closed():
					return
				case <-epoch.ctrl.Closed():
					s.logger.Error("Epoch [%v] died [%v]. Rebuilding.", iter, epoch.ctrl.Failure())
					continue
				case req := <-s.read:
					s.handleRead(epoch, req)
				case req := <-s.swap:
					s.handleSwap(epoch, req)
				}
			}
		}
	}()
}

func (h *machine) sendRequest(ch chan<- *common.Request, cancel <-chan struct{}, val interface{}) (interface{}, error) {
	req := common.NewRequest(val)
	defer req.Cancel()

	select {
	case <-h.ctrl.Closed():
		return nil, errors.WithStack(common.ClosedError)
	case <-cancel:
		return nil, errors.WithStack(common.CanceledError)
	case ch <- req:
		select {
		case <-h.ctrl.Closed():
			return nil, errors.WithStack(common.CanceledError)
		case r := <-req.Acked():
			return r, nil
		case e := <-req.Failed():
			return nil, e
		case <-cancel:
			return nil, errors.WithStack(common.CanceledError)
		}
	}
}

func (s *machine) Read(cancel <-chan struct{}, read getRpc) (Item, bool, error) {
	raw, err := s.sendRequest(s.read, cancel, read)
	if err != nil {
		return Item{}, false, err
	}

	rpc := raw.(responseRpc)
	return rpc.Item, rpc.Ok, nil
}

func (s *machine) Swap(cancel <-chan struct{}, swap swapRpc) (Item, bool, error) {
	raw, err := s.sendRequest(s.swap, cancel, swap)
	if err != nil {
		return Item{}, false, err
	}

	rpc := raw.(responseRpc)
	return rpc.Item, rpc.Ok, nil
}

func (s *machine) Update(cancel <-chan struct{}, key []byte, fn func([]byte) []byte) (Item, error) {
	for !common.IsCanceled(cancel) {
		item, _, err := s.Read(cancel, getRpc{key})
		if err != nil {
			return Item{}, errors.WithStack(err)
		}

		new := fn(item.Val)
		if bytes.Equal(item.Val, new) {
			return item, nil
		}

		item, ok, err := s.Swap(cancel, swapRpc{key, new, item.Ver})
		if err != nil {
			return Item{}, errors.WithStack(err)
		}

		if ok {
			return item, nil
		}
	}
	return Item{}, errors.WithStack(common.CanceledError)
}

func (s *machine) Roster(cancel <-chan struct{}) ([]string, error) {
	item, ok, err := s.Read(cancel, getRpc{[]byte("elmer.roster")})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if !ok {
		return []string{}, nil
	}

	return parseRosterBytes(item.Val)
}

func (s *machine) UpdateRoster(cancel <-chan struct{}, fn func([]string) []string) error {
	_, err := s.Update(cancel, []byte("elmer.roster"), func(cur []byte) []byte {
		roster, err := parseRosterBytes(cur)
		if err != nil || roster == nil {
			roster = []string{}
		}

		next := fn(roster)
		if next == nil {
			return []byte{}
		}

		return peers(next).Bytes()
	})
	return err
}

func (s *machine) handleRead(epoch *epoch, req *common.Request) {
	err := s.pool.SubmitOrCancel(req.Canceled(), func() {
		rpc := req.Body().(getRpc)

		item, ok, err := epoch.Get(req.Canceled(), rpc.Key)
		if err != nil {
			req.Fail(err)
			return
		}

		req.Ack(responseRpc{item, ok})
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting to machine."))
	}
}

func (s *machine) handleSwap(epoch *epoch, req *common.Request) {
	err := s.pool.SubmitOrCancel(req.Canceled(), func() {
		rpc := req.Body().(swapRpc)

		item, ok, err := epoch.Swap(req.Canceled(), Item{rpc.Key, rpc.Val, rpc.Prev})
		if err != nil {
			req.Fail(err)
			return
		}

		req.Ack(responseRpc{item, ok})
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting to machine."))
	}
}

func (s *machine) getLog() (kayak.Log, kayak.Sync, error) {
	sync, err := s.peer.Sync()
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Error retrieving peer syncer [%v]", s.peer.Id())
	}
	log, err := s.peer.Log()
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Error retrieving peer log [%v]", s.peer.Id())
	}
	return log, sync, nil
}

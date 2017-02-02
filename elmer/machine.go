package elmer

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
)

type machine struct {

	ctx    common.Context
	ctrl   common.Control
	logger common.Logger

	peer kayak.Peer

	epoch     *epoch
	epockLock sync.RWMutex

	read chan *common.Request
	swap chan *common.Request

	pool common.WorkPool
}

func (m *machine) Close() error {
	return m.ctrl.Close()
}

func (s *machine) start() error {
	go func() {
		defer s.ctrl.Close()

		for iter := 0; ; iter++ {
			s.logger.Debug("Starting epoch [%v]", iter)

			log, sync, err := s.getLog()
			if err != nil {
				s.logger.Error("Error retrieving log: %v", err)
				continue
			}

			epoch, err := openEpoch(s.ctx, log, sync, iter)
			if err != nil {
				s.logger.Error("Error retrieving log: %v", err)
				continue
			}

			for {
				select {
				case <-s.ctrl.Closed():
					return
				case req := <-s.read:
					s.handleRead(epoch, req)
				case req := <-s.swap:
					s.handleSwap(epoch, req)
				}
			}
		}
	}()
	return nil
}

func (h *machine) sendRequest(ch chan<- *common.Request, timeout time.Duration, val interface{}) (interface{}, error) {
	timer := time.NewTimer(timeout)

	req := common.NewRequest(val)
	select {
	case <-h.ctrl.Closed():
		return nil, ClosedError
	case <-timer.C:
		return nil, errors.Wrapf(TimeoutError, "Request timed out waiting for machine to accept [%v]", timeout)
	case ch <- req:
		select {
		case <-h.ctrl.Closed():
			return nil, ClosedError
		case r := <-req.Acked():
			return r, nil
		case e := <-req.Failed():
			return nil, e
		case <-timer.C:
			req.Cancel()
			return nil, errors.Wrapf(TimeoutError, "Request timed out waiting for machine to response [%v]", timeout)
		}
	}
}

func (s *machine) Read(read getRpc) (Item, bool, error) {
	raw, err := s.sendRequest(s.read, read.Expire, read)
	if err != nil {
		return Item{}, false, err
	}

	rpc := raw.(responseRpc)
	return rpc.Item, rpc.Ok, nil
}

func (s *machine) Swap(swap swapRpc) (Item, bool, error) {
	raw, err := s.sendRequest(s.swap, swap.Expire, swap)
	if err != nil {
		return Item{}, false, err
	}

	rpc := raw.(responseRpc)
	return rpc.Item, rpc.Ok, nil
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

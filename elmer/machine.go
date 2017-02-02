package elmer

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
)

type machine struct {
	ctx    common.Context
	ctrl   common.Control
	logger common.Logger
	peer   kayak.Peer

	epoch     epoch
	epockLock sync.RWMutex

	read chan *common.Request
	swap chan *common.Request

	readTimeout time.Duration
	swapTimeout time.Duration
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

func (s *machine) handleRead(epoch *epoch, req *common.Request) {
	// err := s.pool.SubmitTimeout(s.readTimeout, func() {
		// rpc := req.Body().(getRpc)
		// item, ok := read(idx, rpc.Key)
		// req.Ack(getResponseRpc{item, ok})
	// })
	// if err != nil {
		// req.Fail(errors.Wrapf(err, "Error submitting to machine [%v]", s.readTimeout))
	// }
}

func (s *machine) handleSwap(epoch *epoch, req *common.Request) {
	// err := s.pool.SubmitTimeout(s.readTimeout, func() {
		// rpc := req.Body().(swapRpc)
//
		// item, ok := swap(idx, rpc.Key, rpc.Val, rpc.Exp)
		// req.Ack(getResponseRpc{item, ok})
	// })
	// if err != nil {
		// req.Fail(errors.Wrapf(err, "Error submitting to machine [%v]", s.readTimeout))
	// }
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

func (s *machine) startIndexer(list kayak.Listener, sync kayak.Sync, idx amoeba.Index) common.Control {
	ctrl := s.ctrl.Sub()
	go func() {
		defer list.Close()
	}()
	return ctrl
}

package kayak

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

type syncer struct {
	pool common.ObjectPool // T: *rpcClient
	ref  *ref
}

func newSyncer(pool common.ObjectPool, ref *ref) *syncer {
	return &syncer{pool, ref}
}

func (s *syncer) Barrier(cancel <-chan struct{}) (int, error) {
	for {
		val, err := s.tryBarrier(cancel)
		if err == nil || common.IsCanceled(cancel) {
			return val, nil
		}
	}
}

func (s *syncer) Ack(index int) {
	s.ref.Update(func(cur int) int {
		return common.Max(cur, index)
	})
}

func (s *syncer) Sync(cancel <-chan struct{}, index int) error {
	_, alive := s.ref.WaitUntilOrCancel(cancel, index)
	if !alive {
		return errors.WithStack(ClosedError)
	}

	if common.IsCanceled(cancel) {
		return errors.WithStack(common.CanceledError)
	}

	return nil
}

func (s *syncer) tryBarrier(cancel <-chan struct{}) (val int, err error) {
	raw := s.pool.TakeOrCancel(cancel)
	if raw == nil {
		return 0, errors.WithStack(common.CanceledError)
	}
	defer func() {
		if err != nil {
			s.pool.Fail(raw)
		} else {
			s.pool.Return(raw)
		}
	}()
	val, err = raw.(*rpcClient).Barrier()
	return
}

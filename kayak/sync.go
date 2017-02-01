package kayak

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

type syncer struct {
	pool common.ObjectPool
	ref  *ref
}

func newSyncer(pool common.ObjectPool) *syncer {
	return &syncer{pool, newRef(-1)}
}

func (s *syncer) Barrier(timeout time.Duration) (int, error) {
	for {
		val, err := s.tryBarrier(timeout)
		if err == nil {
			return val, nil
		}
	}
}

func (s *syncer) Ack(index int) {
	s.ref.Update(func(cur int) int {
		return common.Max(cur, index)
	})
}

func (s *syncer) Sync(timeout time.Duration, index int) error {
	_, canceled, alive := s.ref.WaitUntilOrTimeout(timeout, index)
	if !alive {
		return ClosedError
	}
	if canceled {
		return errors.Wrapf(TimeoutError, "Unable to sync. Timeout [%v] while waiting for index [%v] to be applied.", timeout, index)
	}
	return nil
}

func (s *syncer) tryBarrier(timeout time.Duration) (val int, err error) {
	raw := s.pool.TakeTimeout(timeout)
	if raw == nil {
		return 0, errors.Wrapf(TimeoutError, "Unable to retrieve barrier. Timeout [%v] while waiting for client.", timeout)
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

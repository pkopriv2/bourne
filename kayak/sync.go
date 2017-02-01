package kayak

import (
	"time"

	"github.com/pkopriv2/bourne/common"
)

type syncer struct {
	pool common.ObjectPool
	self *replica
	ref  *ref
}

func (s *syncer) Barrier() (int, error) {
	return s.self.ReadBarrier()
}

func (s *syncer) Ack(index int) {
	s.ref.Update(func(cur int) int {
		return common.Max(cur, index)
	})
}

func (s *syncer) Sync(timeout time.Duration, index int) (bool, error) {
	_, canceled, alive := s.ref.WaitUntilOrTimeout(timeout, index)
	if ! alive {
		return false, ClosedError
	}
	return ! canceled, nil
}

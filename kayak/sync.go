package kayak

import "time"

type syncer struct {
}

func (s *syncer) Applied(index int, val interface{}) {
	panic("not implemented")
}

func (s *syncer) Barrier() (int, error) {
	panic("not implemented")
}

func (s *syncer) Sync(timeout time.Duration, barrier int) interface{} {
	panic("not implemented")
}

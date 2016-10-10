package concurrent

import "sync"

type List interface {
	All() []interface{}
	Append(v interface{})
}

type list struct {
	lock  sync.RWMutex
	inner []interface{}
	idx int
}

func NewList(cap int) List {
	return &list{inner: make([]interface{}, 0, cap)}
}

func (s *list) All() []interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return Copylist(s.inner)
}

func (s *list) Append(v interface{}) {
	s.lock.Lock()
	defer s.lock.Lock()
	s.inner[s.idx] = v
	s.idx++
}

func Copylist(orig []interface{}) []interface{} {
	ret := make([]interface{}, len(orig))
	copy(ret, orig)
	return ret
}

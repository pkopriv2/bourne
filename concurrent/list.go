package concurrent

import "sync"

type List interface {
	All() []interface{}
	Append(v interface{})
	Prepend(v interface{})
}

type list struct {
	lock  sync.RWMutex
	inner []interface{}
}

func NewList(cap int) List {
	return &list{inner: make([]interface{}, 0, cap)}
}

func (s *list) All() []interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return CopyList(s.inner)
}

func (s *list) Append(v interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.inner = append(s.inner, v)
}

func (s *list) Prepend(v interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.inner = append([]interface{}{v}, s.inner...)
}

func CopyList(orig []interface{}) []interface{} {
	ret := make([]interface{}, len(orig))
	copy(ret, orig)
	return ret
}

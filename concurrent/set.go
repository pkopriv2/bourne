package concurrent

import "sync"

type Set interface {
	All() []interface{}
	Add(interface{}) bool
	Contains(interface{}) bool
	Remove(interface{}) bool
}

type set struct {
	lock  sync.RWMutex
	inner map[interface{}]struct{}
}

func NewSet() Set {
	return &set{inner: make(map[interface{}]struct{})}
}

func (s *set) All() []interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return CopyArr(Keys(s.inner))
}

func (s *set) Add(v interface{}) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.inner[v]; ok {
		return false
	}

	s.inner[v] = struct{}{}
	return true
}

func (s *set) Contains(v interface{}) bool {
	s.lock.RLock()
	defer s.lock.Unlock()
	var ret bool
	_, ret = s.inner[v]
	return ret
}

func (s *set) Remove(v interface{}) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.inner[v]; ok {
		return false
	}

	delete(s.inner, v)
	return true
}

func Keys(m map[interface{}]struct{}) []interface{} {
	ret := make([]interface{}, 0, len(m))
	for k,_ := range m {
		ret = append(ret, k)
	}
	return ret
}


func CopyArr(orig []interface{}) []interface{} {
	ret := make([]interface{}, 0, len(orig))
	for _,v := range orig {
		ret = append(ret, v)
	}
	return ret
}

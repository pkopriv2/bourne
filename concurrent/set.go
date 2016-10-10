package concurrent

import "sync"

type Set interface {
	All() []interface{}
	Add(interface{})
	Contains(interface{}) bool
	Remove(interface{})
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

func (s *set) Add(v interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.inner[v] = struct{}{}
}

func (s *set) Contains(v interface{}) bool {
	var ret bool
	_, ret = s.inner[v]
	return ret
}

func (s *set) Remove(v interface{}) {
	delete(s.inner, v)
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

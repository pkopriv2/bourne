package utils

import "sync"

type ConcurrentMap struct {
	lock  sync.RWMutex
	inner map[interface{}]interface{}
}

func NewConcurrentMap() *ConcurrentMap {
	return &ConcurrentMap{inner: make(map[interface{}]interface{})}
}

func (s *ConcurrentMap) All() map[interface{}]interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return CopyMap(s.inner)
}

func (s *ConcurrentMap) Get(key interface{}) interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.inner[key]
}

func (s *ConcurrentMap) Put(key interface{}, val interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.inner[key] = &val
}

func (s *ConcurrentMap) Remove(key interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.inner, key)
}

func CopyMap(m map[interface{}]interface{}) map[interface{}]interface{} {
	ret := make(map[interface{}]interface{})
	for k,v := range m {
		ret[k] = v
	}
	return ret
}

package concurrent

import (
	"sync"
)

type Map interface {
	All() map[interface{}]interface{}
	Get(interface{}) interface{}
	Put(interface{}, interface{})
	Remove(interface{})
	Update(func(Map))
}

// concurrent map.
type cmap struct {
	lock  sync.RWMutex
	inner map[interface{}]interface{}
}

func NewMap() Map {
	return &cmap{inner: make(map[interface{}]interface{})}
}

func (s *cmap) Update(fn func(Map)) {
	s.lock.Lock()
	defer s.lock.Unlock()
	fn(&smap{s.inner})
}

func (s *cmap) All() map[interface{}]interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return CopyMap(s.inner)
}

func (s *cmap) Get(key interface{}) interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.inner[key]
}

func (s *cmap) Put(key interface{}, val interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.inner[key] = val
}

func (s *cmap) Remove(key interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.inner, key)
}

// single-threaded map
type smap struct {
	inner map[interface{}]interface{}
}

func (m *smap) All() map[interface{}]interface{} {
	return m.inner
}

func (m *smap) Get(k interface{}) interface{} {
	return m.inner[k]
}

func (m *smap) Put(k interface{}, v interface{}) {
	m.inner[k] = v
}

func (m *smap) Remove(k interface{}) {
	delete(m.inner, k)
}

func (m *smap) Update(func(Map)) {
	panic("Recursive calls to update are not allowed!")
}

func CopyMap(m map[interface{}]interface{}) map[interface{}]interface{} {
	ret := make(map[interface{}]interface{})
	for k, v := range m {
		ret[k] = v
	}
	return ret
}

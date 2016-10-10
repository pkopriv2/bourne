package concurrent

import (
	"fmt"
	"sync"
)

type Map interface {
	All() map[interface{}]interface{}
	Get(interface{}) interface{}
	Put(interface{}, interface{}) error
	Remove(interface{})
}

type mmap struct {
	lock  sync.RWMutex
	inner map[interface{}]interface{}
}

func NewMap() Map {
	return &mmap{inner: make(map[interface{}]interface{})}
}

func (s *mmap) All() map[interface{}]interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return CopyMap(s.inner)
}

func (s *mmap) Get(key interface{}) interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.inner[key]
}

func (s *mmap) Put(key interface{}, val interface{}) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.inner[key]; ok {
		return fmt.Errorf("Key exists")
	}

	s.inner[key] = val
	return nil
}

func (s *mmap) Remove(key interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.inner, key)
}

func CopyMap(m map[interface{}]interface{}) map[interface{}]interface{} {
	ret := make(map[interface{}]interface{})
	for k, v := range m {
		ret[k] = v
	}
	return ret
}

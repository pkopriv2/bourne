package concurrent

import "sync"

type Val interface {
	Set(interface{})
	Get() interface{}
	Swap(interface{}, interface{}) bool
}

type val struct {
	lock  sync.RWMutex
	inner interface{}
}

func NewVal(v interface{}) Val {
	return &val{inner: v}
}

func (v *val) Set(val interface{}) {
	v.lock.Lock()
	defer v.lock.Unlock()
	v.inner = val
}

func (v *val) Get() interface{} {
	v.lock.RLock()
	defer v.lock.RUnlock()
	return v.inner
}

func (v *val) Swap(e interface{}, t interface{}) bool {
	v.lock.Lock()
	defer v.lock.Unlock()
	if v.inner != e {
		return false
	}

	v.inner = t
	return true
}


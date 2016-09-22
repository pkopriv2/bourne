package utils

import (
	"sync/atomic"
	"unsafe"
)

type iface struct {
	typ  unsafe.Pointer
	data unsafe.Pointer
}

type AtomicRef struct {
	v *interface{}
}

func NewAtomicRef() *AtomicRef {
	var v interface{}
	return &AtomicRef{&v}
}

func (v *AtomicRef) Get() unsafe.Pointer {
	vPtr := (*iface)(unsafe.Pointer(v.v))
	return atomic.LoadPointer(&vPtr.data)
}

func (v *AtomicRef) Store(data interface{}) {
	vPtr := (*iface)(unsafe.Pointer(v.v))
	dPtr := (*iface)(unsafe.Pointer(&data))
	atomic.StorePointer(&vPtr.data, dPtr.data)
}

func (v *AtomicRef) Swap(e interface{}, t interface{}) bool {
	vPtr := (*iface)(unsafe.Pointer(v.v))
	ePtr := (*iface)(unsafe.Pointer(&e))
	tPtr := (*iface)(unsafe.Pointer(&t))
	return atomic.CompareAndSwapPointer(&vPtr.data, ePtr.data, tPtr.data)
}

func (v *AtomicRef) Update(fn func(unsafe.Pointer) interface{}) unsafe.Pointer {
	for {
		cur := v.Get()
		nex := fn(cur)
		if v.Swap(cur, nex) {
			return (*iface)(unsafe.Pointer(&nex)).data
		}
	}
}

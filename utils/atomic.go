package utils

import (
	"sync/atomic"
	"unsafe"
)

// this is intended to provide all the missing elements of sync/atomic.

// interface interface{} representation. (could break if this ever changes)
type iface struct {
	typ  unsafe.Pointer
	data unsafe.Pointer
}

type AtomicRef struct {
	v *interface{}
}

func NewAtomicRef(init interface{}) *AtomicRef {
	var v interface{}
	ref := &AtomicRef{&v}
	ref.Store(init)
	return ref
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

type AtomicBool int32

func NewAtomicBool() *AtomicBool {
	var ret AtomicBool
	return &ret
}

func (ab *AtomicBool) Get() bool {
	return atomic.LoadInt32((*int32)(ab)) == 1
}

func (ab *AtomicBool) Set(b bool) {
	if b {
		atomic.StoreInt32((*int32)(ab), 1)
	} else {
		atomic.StoreInt32((*int32)(ab), 0)
	}
}

func (ab *AtomicBool) Swap(e bool, t bool) bool {
	var src int32
	var dst int32

	if e {
		src = 1
	}
	if t {
		dst = 1
	}

	return atomic.CompareAndSwapInt32((*int32)(ab), src, dst)

}

type AtomicCounter struct {
	val *AtomicRef
}

func NewAtomicCounter() *AtomicCounter {
	var zero int
	return &AtomicCounter{NewAtomicRef(zero)}
}

func (a *AtomicCounter) Get() int {
	return *(*int)(a.val.Get())
}

func (a *AtomicCounter) Inc() int {
	return *(*int)(a.val.Update(func(val unsafe.Pointer) interface{} {
		cur := (*int)(val)
		return *cur + 1
	}))
}

func (a *AtomicCounter) Dec() int {
	return *(*int)(a.val.Update(func(val unsafe.Pointer) interface{} {
		cur := (*int)(val)
		return *cur - 1
	}))
}

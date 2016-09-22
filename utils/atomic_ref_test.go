package utils

import (
	"fmt"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

type AtomicCounter struct {
	val *AtomicRef
}

func NewAtomicCounter() *AtomicCounter {
	zero := 0

	ref := NewAtomicRef()
	ref.Store(&zero)
	return &AtomicCounter{ref}
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

func TestAtomicRef_Empty(t *testing.T) {
	ctr := NewAtomicCounter()

	var wait sync.WaitGroup
	for i := 0; i < 100; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for i := 0; i < 1000; i++ {
				fmt.Println(ctr.Inc())
			}
		}()
	}

	wait.Wait()
	assert.Equal(t, 100000, ctr.Get())
}

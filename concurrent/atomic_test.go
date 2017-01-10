package concurrent

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAtomicCounter(t *testing.T) {
	ctr := NewAtomicCounter()

	var wait sync.WaitGroup
	for i := 0; i < 100; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for i := 0; i < 100; i++ {
				ctr.Inc()
			}
		}()
	}

	wait.Wait()
	assert.Equal(t, uint64(10000), ctr.Get())
}

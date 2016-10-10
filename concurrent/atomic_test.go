package concurrent

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAtomicRef_Counter(t *testing.T) {
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
	assert.Equal(t, 10000, ctr.Get())
}

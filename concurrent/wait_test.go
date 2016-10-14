package concurrent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWait_simple(t *testing.T) {
	wait := NewWait()

	wait.Inc()
	go func() {
		defer wait.Dec()

		time.Sleep(1 * time.Second)
		wait.Inc()
		go func() {
			defer wait.Dec()
		}()
	}()

	wait.Inc()
	go func() {
		wait.Dec()
	}()

	assert.NotNil(t, <-wait.Wait())

}

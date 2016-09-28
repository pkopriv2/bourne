package tunnel

import (
	"io"
	"time"

	"github.com/pkopriv2/bourne/utils"
)

func NewBufferer(env *Env, in chan []byte) (io.Reader, func(utils.StateController, []interface{})) {
	stream := NewStream(env.conf.BuffererMax)

	return stream, func(state utils.StateController, args []interface{}) {
		for {

			var cur []byte
			select {
			case <-state.Done():
				return
			case cur = <-in:
			}

			done, timer := NewCircuitBreaker(365 * 24 * time.Hour, func() { stream.Write(cur) })
			select {
			case <-state.Done():
				return
			case <-done:
				continue
			case <-timer:
				state.Fail(NewTimeoutError("BUFFERER(Timeout delivering data)"))
		}
	}
}
}

func NewCircuitBreaker(dur time.Duration, fn func()) (<-chan bool, <-chan time.Time) {
	timer := time.After(dur)
	done := make(chan bool)

	go func() {
		fn()
		done<-true
	}()

	return done, timer
}

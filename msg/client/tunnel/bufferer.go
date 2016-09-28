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

			done, timer := utils.NewCircuitBreaker(365*24*time.Hour, func() { stream.Write(cur) })
			select {
			case <-state.Done():
				return
			case <-done:
				continue
			case <-timer:
				state.Fail(NewTimeoutError("BUFFERER(Timeout delivering data)"))
				return
			}
		}
	}
}

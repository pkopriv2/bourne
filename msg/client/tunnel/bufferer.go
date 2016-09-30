package tunnel

import (
	"time"

	"github.com/pkopriv2/bourne/utils"
)

func NewRecvBuffer(env *tunnelEnv, channels *tunnelChannels) (*Stream, func(utils.Controller, []interface{})) {
	stream := NewStream(env.config.BuffererLimit)

	return stream, func(state utils.Controller, args []interface{}) {
		defer env.logger.Info("Bufferer closing")
		defer stream.Close()
		for {
			var cur []byte
			select {
			case <-state.Close():
				return
			case cur = <-channels.bufferer:
			}

			done, timer := utils.NewCircuitBreaker(365*24*time.Hour, func() { stream.Write(cur) })
			select {
			case <-state.Close():
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

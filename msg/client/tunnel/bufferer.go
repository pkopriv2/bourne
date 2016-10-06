package tunnel

import (
	"time"
	"fmt"

	"github.com/pkopriv2/bourne/msg/core"
	"github.com/pkopriv2/bourne/utils"
)

type BuffererSocket struct {
	SegmentTx <-chan []byte
}

func NewRecvBuffer(ctx core.Context, socket *BuffererSocket, stream *Stream) func(utils.WorkerController, []interface{}) {
	logger := ctx.Logger()

	return func(state utils.WorkerController, args []interface{}) {
		logger.Debug("RecvBuffer Opened")
		defer logger.Info("RecvBuffer Closed")

		var cur []byte
		for {
			select {
			case <-state.Close():
				return
			case cur = <-socket.SegmentTx:
			}

			done, timer := utils.NewCircuitBreaker(365*24*time.Hour, func() { stream.Write(cur) })
			select {
			case <-state.Close():
				return
			case <-done:
				continue
			case <-timer:
				state.Fail(fmt.Errorf("Timeout delivering data to stream.  Consumer may have left"))
				return
			}
		}
	}
}

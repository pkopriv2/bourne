package tunnel

import (
	"fmt"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/machine"
	"github.com/pkopriv2/bourne/utils"
)

type BuffererSocket struct {
	SegmentTx <-chan []byte
}

func NewBufferer(ctx common.Context, socket *BuffererSocket, stream *concurrent.Stream) func(machine.WorkerSocket, []interface{}) {
	logger := ctx.Logger()

	return func(state machine.WorkerSocket, args []interface{}) {
		logger.Debug("RecvBuffer Opened")
		defer logger.Info("RecvBuffer Closed")

		var cur []byte
		for {
			select {
			case <-state.Closed():
				return
			case cur = <-socket.SegmentTx:
			}

			done, timer := utils.NewCircuitBreaker(365*24*time.Hour, func() { stream.Write(cur) })
			select {
			case <-state.Closed():
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

package tunnel

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/machine"
)

func NewFailer(ctx common.Context, streamTx concurrent.Stream, streamRx concurrent.Stream) func(machine.WorkerSocket, []interface{}) {
	logger := ctx.Logger()

	return func(state machine.WorkerSocket, args []interface{}) {
		logger.Debug("Failer Opening")
		defer logger.Debug("Failer Closing")

		err := args[0].(error)

		streamTx.Close()
		streamRx.Close()

		state.Fail(err)
	}
}

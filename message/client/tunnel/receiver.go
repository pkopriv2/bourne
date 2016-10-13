package tunnel

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/machine"
	"github.com/pkopriv2/bourne/message/wire"
)

type ReceiverSocket struct {
	PacketRx  <-chan wire.Packet
	SegmentTx chan<- wire.SegmentMessage
	VerifyTx  chan<- wire.NumMessage
}

func NewReceiver(ctx common.Context, socket *ReceiverSocket) func(machine.WorkerSocket, []interface{}) {
	logger := ctx.Logger()

	return func(state machine.WorkerSocket, args []interface{}) {
		logger.Debug("ReceiveMain Starting")
		defer logger.Debug("ReceiveMain Closing")

		var chanIn <-chan wire.Packet
		var chanAssembler chan<- wire.SegmentMessage
		var chanVerifier chan<- wire.NumMessage

		var msgVerify wire.NumMessage
		var msgSegment wire.SegmentMessage

		for {
			if msgVerify == nil && msgSegment == nil {
				chanIn = socket.PacketRx
			} else {
				chanIn = nil
			}

			if msgVerify != nil {
				chanVerifier = socket.VerifyTx
			} else {
				chanVerifier = nil
			}

			if msgSegment != nil {
				chanAssembler = socket.SegmentTx
			} else {
				chanAssembler = nil
			}

			select {
			case <-state.Closed():
				return
			case chanAssembler <- msgSegment:
				msgSegment = nil
				break
			case chanVerifier <- msgVerify:
				msgVerify = nil
				break
			case p := <-chanIn:
				// Handle: close
				if close := p.Close(); close != nil {
					state.Next(TunnelClosingRecv, p.Close().Val())
					return
				}

				// Handle: error
				if err := p.Error(); err != nil {
					state.Fail(err)
					return
				}

				// Handle: verify message
				if verify := p.Verify(); verify != nil {
					msgVerify = verify
				}

				// Handle: segment
				if segment := p.Segment(); segment != nil {
					msgSegment = segment
				}
			}
		}
	}
}

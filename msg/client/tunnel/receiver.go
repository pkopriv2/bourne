package tunnel

import (
	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
)

func NewRecvMain(env *tunnelEnv, channels *tunnelChannels) func(utils.Controller, []interface{}) {
	return func(state utils.Controller, args []interface{}) {
		defer env.logger.Info("Receiver closing")

		var chanIn <-chan wire.Packet
		var chanAssembler chan<- wire.SegmentMessage
		var chanVerifier chan<- wire.NumMessage

		var msgVerify wire.NumMessage
		var msgSegment wire.SegmentMessage

		for {
			if msgVerify == nil && msgSegment == nil {
				chanIn = channels.recvMain
			} else {
				chanIn = nil
			}

			if msgVerify != nil {
				chanVerifier = channels.sendVerifier
			} else {
				chanVerifier = nil
			}

			if msgSegment != nil {
				chanAssembler = channels.assembler
			} else {
				chanAssembler = nil
			}

			select {
			case <-state.Close():
				return
			case chanAssembler <- msgSegment:
				break
			case chanVerifier <- msgVerify:
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

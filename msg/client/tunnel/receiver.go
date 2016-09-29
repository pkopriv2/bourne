package tunnel

import (
	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
)

func NewReceiver(env *Env, in <-chan wire.Packet, assembler chan<- wire.SegmentMessage, verifier chan<- wire.NumMessage) func(utils.StateController, []interface{}) {
	return func(state utils.StateController, args []interface{}) {
		defer env.Log("Receiver closing")

		var chanIn <-chan wire.Packet
		var chanAssembler chan<-wire.SegmentMessage
		var chanVerifier chan<-wire.NumMessage

		var msgVerify wire.NumMessage
		var msgSegment wire.SegmentMessage

		for {
			if msgVerify == nil && msgSegment == nil {
				chanIn = in
			}

			if msgVerify != nil {
				chanVerifier = verifier
			}

			if msgSegment != nil {
				chanAssembler = assembler
			}

			select {
			case <-state.Done():
				return
			case chanAssembler<-msgSegment:
				break
			case chanVerifier<-msgVerify:
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

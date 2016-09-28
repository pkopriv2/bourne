package tunnel

import (
	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
)

func NewReceiver(env *Env, in chan wire.Packet, assembler chan wire.SegmentMessage, ack chan uint64) func(utils.StateController, []interface{}) {
	return func(state utils.StateController, args []interface{}) {
		for {
			var p wire.Packet
			select {
			case <-state.Done():
				return
			case p = <-in:
			}

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
				select {
				case <-state.Done():
					return
				case ack <- verify.Val():
				}
			}

			// Handle: segment
			if segment := p.Segment(); segment != nil {
				select {
				case <-state.Done():
					return
				case assembler <- segment:
				}
			}
		}
	}
}

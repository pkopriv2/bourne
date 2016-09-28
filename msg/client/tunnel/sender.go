package tunnel

import (
	"time"

	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
)

func NewSender(env *Env, prune <-chan uint64, verify <-chan uint64, out chan<- wire.Packet) (*Stream, func(utils.StateController, []interface{})) {
	stream := NewStream(env.conf.BuffererMax)

	return stream, func(state utils.StateController, args []interface{}) {
		defer env.Log("Sender closing")

		in := tryReadStream(stream)

		// track time between received verifications.
		var timeout <-chan time.Time
		var timeoutCur time.Duration
		var timeoutCnt int
		timeoutReset := func() {
			timeoutCnt = 0
			timeoutCur = env.conf.ackTimeout
			timeout = time.After(timeoutCur)
		}

		var curIn <-chan input
		var curOut chan<- wire.Packet

		packet := wire.BuildPacket(env.route).Build()
		for {

			// only allow input if we don't already have a segment to deal with!
			if packet.Segment() != nil {
				curIn = nil
			} else {
				curIn = in
			}

			// verify timeouts can only occur if we've sent something!
			tail, cur, _, _ := stream.Snapshot()
			if cur.offset == tail.offset {
				timeout = nil
			}

			select {
			case <-state.Done():
				return
			case <-timeout:
				if timeoutCnt++; timeoutCnt >= env.conf.maxRetries {
					state.Fail(NewTimeoutError("Too many ack timeouts"))
					return
				}

				// exponential backoff
				timeoutCur *= 2
				timeout = time.After(timeoutCur)
			case offset := <-prune:
				if _, err := stream.Commit(offset); err != nil {
					state.Fail(err)
					return
				}

				timeoutReset()
			case offset := <-verify:
				packet = packet.Update().SetVerify(offset).Build()
			case input := <-curIn:
				if input.err != nil {
					state.Fail(input.err)
					return
				}

				packet = packet.Update().SetSegment(input.segment.offset, input.segment.data).Build()
			case curOut<-packet:
				packet = wire.BuildPacket(env.route).Build()
			}
		}
	}
}

type segment struct {
	offset uint64
	data []byte
}

type input struct {
	err error
	segment *segment
}

func tryReadStream(stream *Stream) <-chan input {
	data := make(chan input)

	buf := make([]byte, wire.PacketMaxSegmentLength)
	go func() {
		for {
			ref, num, e := stream.TryRead(buf, false)
			if e != nil {
				data <- input{e, nil}
				return
			}

			data <- input{segment: &segment{ref.offset, buf[:num]}}
		}
	}()

	return data
}

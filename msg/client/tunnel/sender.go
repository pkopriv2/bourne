package tunnel

import (
	"fmt"
	"time"

	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
)

func NewSendMain(route wire.Route, env *tunnelEnv, channels *tunnelChannels) (*Stream, func(utils.Controller, []interface{})) {
	stream := NewStream(env.config.SenderLimit)

	return stream, func(state utils.Controller, args []interface{}) {
		defer env.logger.Info("Sender closing")
		defer stream.Close()

		// wrap the stream in a channel
		in := readStream(stream)

		// track time between send verifications.
		var timeout <-chan time.Time
		var timeoutCur time.Duration
		var timeoutCnt int

		var chanIn <-chan input
		var chanOut chan<- wire.Packet

		packet := wire.BuildPacket(route).Build()
		for {
			// timeouts only apply if we've sent data.
			tail, cur, _, _ := stream.Snapshot()
			if cur.offset == tail.offset {
				timeout = nil
			}

			// we can send any non-empty packet
			if !packet.Empty() {
				chanOut = channels.sendMain
			}

			// we can read input as long as we don't have a segment
			if packet.Segment() == nil {
				chanIn = in
			} else {
				chanIn = nil
			}

			select {
			case <-state.Close():
				return
			case <-timeout:
				if timeoutCnt++; timeoutCnt >= env.config.MaxRetries {
					state.Fail(NewTimeoutError("Too many ack timeouts"))
					return
				}

				// exponential backoff
				timeoutCur *= 2
				timeout = time.After(timeoutCur)
			case msg := <-channels.sendVerifier:
				if _, err := stream.Commit(msg.Val()); err != nil {
					state.Fail(err)
					return
				}
				fmt.Println(stream.Data())

				timeoutCnt = 0
				timeoutCur = env.config.VerifyTimeout
				timeout = time.After(timeoutCur)
			case msg := <-channels.recvVerifier:
				packet = packet.Update().SetVerify(msg.Val()).Build()
			case input := <-chanIn:
				if input.err != nil {
					state.Fail(input.err)
					return
				}

				packet = packet.Update().SetSegment(input.segment.offset, input.segment.data).Build()
			case chanOut <- packet:
				packet = wire.BuildPacket(route).Build()
			}
		}
	}
}

type segment struct {
	offset uint64
	data   []byte
}

type input struct {
	err     error
	segment *segment
}

func readStream(stream *Stream) <-chan input {
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

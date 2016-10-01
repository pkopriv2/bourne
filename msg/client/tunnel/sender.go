package tunnel

import (
	"time"

	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
)

func NewSendMain(route wire.Route, env *tunnelEnv, channels *tunnelChannels) (*Stream, func(utils.Controller, []interface{})) {
	stream := NewStream(env.config.SenderLimit)

	return stream, func(state utils.Controller, args []interface{}) {
		env.logger.Debug("SendMain Starting")
		defer env.logger.Info("SendMain Closing")

		// wrap the stream in a channel
		in := readStream(stream)

		// track time between send verifications.
		var timeout <-chan time.Time
		var timeoutCur time.Duration
		var timeoutCnt int

		resetTimeout := func() {
			timeoutCnt = 0
			timeoutCur = env.config.VerifyTimeout
			timeout = time.After(timeoutCur)
		}

		var chanIn <-chan input
		var chanOut chan<- wire.Packet

		packet := wire.BuildPacket(route).Build()
		for {

			// timeouts only apply if we've sent data.
			tail, _, head, _ := stream.Snapshot()
			if head.offset == tail.offset {
				timeout = nil
			}

			// we can send any non-empty packet
			if !packet.Empty() {
				chanOut = channels.sendMain
			} else {
				chanOut = nil
			}

			// we can read input as long as we don't have a segment
			if segment := packet.Segment(); segment == nil {
				chanIn = in
			} else {
				chanIn = nil
			}

			select {
			case <-state.Close():
				return
			case <-timeout:
				stream.Reset()
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

				resetTimeout()
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
				if timeout == nil {
					resetTimeout()
				}
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

			if num == 0 {
				time.Sleep(10 * time.Millisecond)
				continue
			}

			tmp := make([]byte, num)
			copy(tmp, buf[:num])

			data <- input{segment: &segment{ref.offset, tmp}}
		}
	}()

	return data
}

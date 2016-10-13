package tunnel

import (
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/machine"
	"github.com/pkopriv2/bourne/message/wire"
)

type SenderSocket struct {
	PacketTx     chan<- wire.Packet
	SendVerifyRx <-chan wire.NumMessage
	RecvVerifyRx <-chan wire.NumMessage
}

func NewSender(route wire.Route, ctx common.Context, stream *concurrent.Stream, socket *SenderSocket) func(machine.WorkerSocket, []interface{}) {
	logger := ctx.Logger()
	config := ctx.Config()

	confTimeout := config.OptionalDuration(confTunnelVerifyTimeout, defaultTunnelVerifyTimeout)
	confTries := config.OptionalInt(confTunnelMaxRetries, defaultTunnelMaxRetries)
	return func(state machine.WorkerSocket, args []interface{}) {
		logger.Debug("SendMain Starting")
		defer logger.Info("SendMain Closing")

		// wrap the stream in a channel
		in := readStream(stream)

		// track time between send verifications.
		var timeout <-chan time.Time
		var timeoutCur time.Duration
		var timeoutCnt int

		resetTimeout := func() {
			timeoutCnt = 0
			timeoutCur = confTimeout
			timeout = time.After(timeoutCur)
		}

		var chanIn <-chan input
		var chanOut chan<- wire.Packet

		packet := wire.BuildPacket(route).Build()
		for {
			// timeouts only apply if we've sent data.
			tail, _, head, _ := stream.Snapshot()
			if head.Offset == tail.Offset {
				timeout = nil
			}

			// we can send any non-empty packet
			if !packet.Empty() {
				chanOut = socket.PacketTx
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
			case <-state.Closed():
				return
			case <-timeout:
				stream.Reset()
				if timeoutCnt++; timeoutCnt >= confTries {
					state.Fail(NewTimeoutError("Too many ack timeouts"))
					return
				}

				// exponential backoff
				timeoutCur *= 2
				timeout = time.After(timeoutCur)
			case msg := <-socket.SendVerifyRx:
				if _, err := stream.Commit(msg.Val()); err != nil {
					state.Fail(err)
					return
				}

				resetTimeout()
			case msg := <-socket.RecvVerifyRx:
				packet = packet.Update().SetVerify(msg.Val()).Build()
			case input := <-chanIn:
				if input.err != nil {
					state.Fail(input.err)
					return
				}

				packet = packet.Update().SetSegment(input.segment.offset, input.segment.data).Build()
				resetTimeout()
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

func readStream(stream *concurrent.Stream) <-chan input {
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

			data <- input{segment: &segment{ref.Offset, tmp}}
		}
	}()

	return data
}

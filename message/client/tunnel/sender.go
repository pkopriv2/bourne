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
}

func NewSender(route wire.Route, ctx common.Context, stream *concurrent.Stream, socket *SenderSocket) func(machine.WorkerSocket, []interface{}) {
	logger := ctx.Logger()
	config := ctx.Config()

	confTimeout := config.OptionalDuration(confTunnelVerifyTimeout, defaultTunnelVerifyTimeout)
	confTries := config.OptionalInt(confTunnelMaxRetries, defaultTunnelMaxRetries)
	return func(state machine.WorkerSocket, args []interface{}) {
		logger.Debug("SendMain Starting")
		defer logger.Info("SendMain Closing")

		// track time between send verifications.
		var sendTimeout <-chan time.Time
		var sendTimeoutCur time.Duration
		var sendTimeoutCnt int
		sendTimerReset := func() {
			sendTimeoutCnt = 0
			sendTimeoutCur = confTimeout
			sendTimeout = time.After(confTimeout)
		}
		sendTimerBackoff := func() {
			sendTimeoutCnt += 1
			sendTimeoutCur *= 2
			sendTimeout = time.After(sendTimeoutCur)
		}
		sendTimerDisable := func() {
			sendTimeoutCnt = 0
			sendTimeoutCur = confTimeout
			sendTimeout = nil
		}

		in := readStream(stream)
		chanIn := in
		chanOut := socket.PacketTx

		packet := wire.BuildPacket(route).Build()
		for {
			// send timeouts only apply if we've sent data.
			tail, _, head, _ := stream.Snapshot()
			if head.Offset == tail.Offset {
				sendTimerDisable()
			}

			// we can read input as long as we don't have a segment
			if segment := packet.Segment(); segment == nil {
				chanIn = in
				chanOut = nil
			} else {
				chanIn = nil
				chanOut = socket.PacketTx
			}

			select {
			case <-state.Closed():
				return
			case <-sendTimeout:
				if _,_,err := stream.Reset(); err != nil {
					state.Fail(err)
					return
				}

				sendTimerBackoff()
				if sendTimeoutCnt >= confTries {
					state.Fail(NewTimeoutError("Too many ack timeouts"))
					return
				}
			case msg := <-socket.SendVerifyRx:
				if _, err := stream.Commit(msg.Val()); err != nil {
					state.Fail(err)
					return
				}

				sendTimerReset()
			case input := <-chanIn:
				if input.err != nil {
					state.Fail(input.err)
					return
				}

				packet = packet.Update().SetSegment(input.segment.Offset(), input.segment.Data()).Build()
			case chanOut <- packet:
				packet = wire.BuildPacket(route).Build()
				sendTimerReset()
			}
		}
	}
}

type input struct {
	err     error
	segment wire.SegmentMessage
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
				time.Sleep(50 * time.Millisecond)
				continue
			}

			tmp := make([]byte, num)
			copy(tmp, buf[:num])

			data <- input{segment: wire.NewSegmentMessage(ref.Offset, tmp)}
		}
	}()

	return data
}

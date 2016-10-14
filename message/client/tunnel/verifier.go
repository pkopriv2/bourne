package tunnel

import (
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/machine"
	"github.com/pkopriv2/bourne/message/wire"
)

type VerifierSocket struct {
	PacketTx     chan<- wire.Packet
	RecvVerifyRx <-chan wire.NumMessage
}

func NewVerifier(route wire.Route, ctx common.Context, socket *VerifierSocket) func(machine.WorkerSocket, []interface{}) {
	logger := ctx.Logger()
	config := ctx.Config()

	confTimeout := config.OptionalDuration(confTunnelVerifyTimeout, defaultTunnelVerifyTimeout) / 4
	return func(state machine.WorkerSocket, args []interface{}) {
		logger.Debug("Verifier Starting")
		defer logger.Debug("Verifier Closing")

		receiver := NewVerifyReceiver()
		go receiver.Run(state.Closed(), socket.RecvVerifyRx)

		var last wire.NumMessage
		buildLatestPacket := func() wire.Packet {
			latest := receiver.Latest()
			if latest == nil {
				return nil
			}

			if latest == last {
				return nil
			}

			last = latest
			return wire.BuildPacket(route).SetVerify(latest.Val()).Build()
		}

		var timer = time.After(confTimeout)
		var packet wire.Packet
		var packetTx chan<-wire.Packet
		for {

			if packet == nil {
				packetTx = nil
			} else {
				packetTx = socket.PacketTx
			}

			select {
			case <-state.Closed():
				return
			case <-receiver.Trigger():
				packet = buildLatestPacket()
				continue
			case packetTx <- packet:
				timer = time.After(confTimeout)
				packet = nil
				continue
			case <-timer:
				packet = buildLatestPacket()
				continue
			}
		}
	}
}

type VerifyReceiver struct {
	verifyTx     chan struct{}
	verifyLatest concurrent.Val
}

func NewVerifyReceiver() *VerifyReceiver {
	return &VerifyReceiver{
		verifyTx:     make(chan struct{}, 1),
		verifyLatest: concurrent.NewVal((wire.NumMessage)(nil))}
}

func (v *VerifyReceiver) Latest() wire.NumMessage {
	latest, ok := v.verifyLatest.Get().(wire.NumMessage)
	if !ok {
		return nil
	}

	return latest
}

func (v *VerifyReceiver) Trigger() <-chan struct{} {
	return v.verifyTx
}

func (v *VerifyReceiver) Run(close <-chan struct{}, verifyRx <-chan wire.NumMessage) {
	triggered := false
	triggerTx := v.verifyTx
	cnt := 0
	for {
		if triggered {
			triggerTx = v.verifyTx
		} else {
			triggerTx = nil
		}

		select {
		case <-close:
			return
		case triggerTx <- struct{}{}:
			triggered = false
			continue
		case val := <-verifyRx:
			v.verifyLatest.Set(val)
			if cnt++; cnt < 5 {
				continue
			}

			cnt = 0
			triggered = true
		}
	}
}

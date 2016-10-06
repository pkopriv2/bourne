package tunnel

import (
	"fmt"
	"math/rand"

	"github.com/pkopriv2/bourne/msg/core"
	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
)


type CloserSocket struct {
	PacketRx <-chan wire.Packet
	PacketTx chan<- wire.Packet
}

func NewCloserInit(route wire.Route, ctx core.Context, socket *CloserSocket) func(utils.WorkerController, []interface{}) {
	return func(state utils.WorkerController, args []interface{}) {
		if err := closeInit(route, ctx, socket.PacketRx, socket.PacketTx); err != nil {
			state.Fail(err)
		}

		state.Next(TunnelClosed)
	}
}

func NewCloserRecv(route wire.Route, ctx core.Context, socket *CloserSocket) func(utils.WorkerController, []interface{}) {
	return func(state utils.WorkerController, args []interface{}) {
		challenge := args[0].(uint64)

		if err := closeRecv(route, ctx, socket.PacketRx, socket.PacketTx, challenge); err != nil {
			state.Fail(err)
		}

		state.Next(TunnelClosed)
	}
}

// Performs close handshake from initiator's perspective: send(close), recv(close, verify), send(verify)
func closeInit(route wire.Route, ctx core.Context, in <-chan wire.Packet, out chan<- wire.Packet) error {

	// generate a new random value for the handshake.
	offset := uint64(rand.Uint32())

	// Send: close
	if err := sendOrTimeout(ctx, out, wire.BuildPacket(route).SetClose(offset).Build()); err != nil {
		return NewClosingError(fmt.Sprintf("Failed: send(close): %v", err.Error()))
	}

	// Receive: close, verify (drop any non close packets)
	var p wire.Packet
	var err error
	for {
		p, err = recvOrTimeout(ctx, in)
		if err != nil || p == nil {
			return NewClosingError("Timeout: recv(close,verify)")
		}

		close, verify := p.Close(), p.Verify()
		if close == nil || verify == nil {
			continue
		}

		if verify.Val() == offset {
			break
		}
	}

	// Send: verify
	out <- wire.BuildPacket(route).SetClose(p.Close().Val()).Build()

	return nil
}

// Performs receiver (ie listener) close handshake: recv(close)[already received], send(close,verify), recv(verify)
func closeRecv(route wire.Route, ctx core.Context, in <-chan wire.Packet, out chan<- wire.Packet, challenge uint64) error {

	// Send: close verify
	offset := uint64(rand.Uint32())

	if err := sendOrTimeout(ctx, out, wire.BuildPacket(route).SetClose(offset).SetVerify(challenge).Build()); err != nil {
		return NewClosingError(fmt.Sprintf("Failed: send(close,verify): %v", err.Error()))
	}

	// Receive: verify
	p, err := recvOrTimeout(ctx, in)
	if err != nil {
		return NewClosingError(fmt.Sprintf("Failed: receive(verify): %v", err.Error()))
	}

	if verify := p.Verify(); verify == nil || verify.Val() != offset {
		return NewClosingError("Failed: receive(verify). Incorrect value")
	}

	return nil
}

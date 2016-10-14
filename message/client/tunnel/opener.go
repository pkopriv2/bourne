package tunnel

import (
	"fmt"
	"math/rand"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/machine"
	"github.com/pkopriv2/bourne/message/core"
	"github.com/pkopriv2/bourne/message/wire"
)

func NewOpenerInit(route wire.Route, ctx common.Context, socket core.DataSocket) func(machine.WorkerSocket, []interface{}) {
	logger := common.FormatLogger(ctx.Logger(), route)
	tries := ctx.Config().OptionalInt(confTunnelMaxRetries, defaultTunnelMaxRetries)
	return func(state machine.WorkerSocket, args []interface{}) {
		logger.Debug("Opener Init Started")

		var err error
		for i := 0; i < tries; i++ {
			if err = openInit(route, ctx, socket.Rx(), socket.Tx()); err == nil {
				state.Next(TunnelOpened)
				return
			}

			logger.Debug("Error for attempt [%v]: %v", i, err)
		}

		state.Fail(err)
	}
}

func NewOpenerRecv(route wire.Route, ctx common.Context, socket core.DataSocket) func(machine.WorkerSocket, []interface{}) {
	logger := common.FormatLogger(ctx.Logger(), route)
	tries := ctx.Config().OptionalInt(confTunnelMaxRetries, defaultTunnelMaxRetries)

	return func(state machine.WorkerSocket, args []interface{}) {
		logger.Debug("Opener Recv Started")

		var err error
		for i := 0; i < tries; i++ {
			if err = openRecv(route, ctx, socket.Rx(), socket.Tx()); err == nil {
				state.Next(TunnelOpened)
				return
			}

			logger.Debug("Error for attempt [%v]: %v", i, err)
		}

		state.Fail(err)
	}
}

// Performs open handshake: send(open), recv(open,verify), send(verify)
func openInit(route wire.Route, ctx common.Context, rx <-chan wire.Packet, tx chan<- wire.Packet) error {
	logger := common.FormatLogger(ctx.Logger(), route)
	logger.Info("Initiating open request")

	var p wire.Packet
	var err error

	// generate a new offset for the handshake.
	offset := uint64(rand.Uint32())

	// Send: open
	if err = sendOrTimeout(ctx, tx, wire.BuildPacket(route).SetOpen(offset).Build()); err != nil {
		return NewOpeningError(fmt.Sprintf("Failed: send(open): %v", err.Error()))
	}

	// Receive: open, verify
	p, err = recvOrTimeout(ctx, rx)
	if err != nil || p == nil {
		ret := NewOpeningError(fmt.Sprintf("Failed: recv(open, verify): %v", err.Error()))
		// sendOrTimeout(ctx, tx, wire.BuildPacket(route).SetError(ret).Build())
		return ret
	}

	open, verify := p.Open(), p.Verify()
	if open == nil || verify == nil {
		ret := NewOpeningError(fmt.Sprintf("Failed: recv(open, verify): Missing open or verify: %v", p))
		// ret := NewOpeningError("Failed: recv(open, verify): Missing open or verify:")
		// sendOrTimeout(ctx, tx, wire.BuildPacket(route).SetError(ret).Build())
		return ret
	}

	if verify.Val() != offset {
		ret := NewOpeningError(fmt.Sprintf("Failed: receive(open, verify): Incorrect verify [%v]", verify.Val()))
		// sendOrTimeout(ctx, tx, wire.BuildPacket(route).SetError(ret).Build())
		return ret
	}

	// Send: verify
	if err = sendOrTimeout(ctx, tx, wire.BuildPacket(route).SetVerify(open.Val()).Build()); err != nil {
		ret := NewOpeningError(fmt.Sprintf("Failed: send(verify): %v", err.Error()))
		// sendOrTimeout(ctx, tx, wire.BuildPacket(route).SetError(ret).Build())
		return ret
	}

	return nil
}

// Performs receiver (ie listener) open handshake: recv(open), send(open,verify), recv(verify)
func openRecv(route wire.Route, ctx common.Context, rx <-chan wire.Packet, tx chan<- wire.Packet) error {
	logger := common.FormatLogger(ctx.Logger(), route)
	logger.Info("Waiting for open request")

	// Receive: open
	logger.Info("Receive (open)")
	p, err := recvOrTimeout(ctx, rx)
	if err != nil {
		return NewOpeningError(fmt.Sprintf("Failed: recv(open): %v", err))
	}

	open := p.Open()
	if open == nil {
		ret := NewOpeningError("Failed: recv(open): Missing open message.")
		sendOrTimeout(ctx, tx, wire.BuildPacket(route).SetError(ret).Build())
		return ret
	}

	// Send: open,verify
	logger.Debug("Sending (open,verify)")
	offset := uint64(rand.Uint32())
	if err := sendOrTimeout(ctx, tx, wire.BuildPacket(route).SetVerify(open.Val()).SetOpen(offset).Build()); err != nil {
		ret := NewOpeningError(fmt.Sprint("Failed: send(open,verify): %v", err))
		sendOrTimeout(ctx, tx, wire.BuildPacket(route).SetError(ret).Build())
		return ret
	}

	// Receive: verify
	logger.Debug("Receive (verify)")
	p, err = recvOrTimeout(ctx, rx)
	if err != nil {
		ret := NewOpeningError(fmt.Sprint("Failed: recv(verify): %v", err))
		sendOrTimeout(ctx, tx, wire.BuildPacket(route).SetError(ret).Build())
		return ret
	}

	verify := p.Verify()
	if verify == nil || verify.Val() != offset {
		ret := NewOpeningError("Failed: recv(verify): Missing or incorrect verify")
		sendOrTimeout(ctx, tx, wire.BuildPacket(route).SetError(ret).Build())
		return ret
	}

	return nil
}

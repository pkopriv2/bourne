package tunnel

import (
	"fmt"
	"math/rand"

	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
)

// TODO: Due to quick refactor, these workers don't respond to controller signals yet.
func NewOpenerInit(route wire.Route, env *tunnelEnv, channels *tunnelChannels) func(utils.Controller, []interface{}) {
	return func(state utils.Controller, args []interface{}) {
		var err error

		for i := 0; i < env.config.MaxRetries; i++ {
			if err = openInit(route, env, channels.recvMain, channels.sendMain); err == nil {
				state.Next(TunnelOpened)
				return
			}
		}

		state.Fail(err)
	}
}

func NewOpenerRecv(route wire.Route, env *tunnelEnv, channels *tunnelChannels) func(utils.Controller, []interface{}) {
	return func(state utils.Controller, args []interface{}) {
		var err error

		for i := 0; i < env.config.MaxRetries; i++ {
			if err = openRecv(route, env, channels.recvMain, channels.sendMain); err == nil {
				state.Next(TunnelOpened)
				return
			}
		}

		state.Fail(err)
	}
}

// Performs open handshake: send(open), recv(open,verify), send(verify)
func openInit(route wire.Route, env *tunnelEnv, in <-chan wire.Packet, out chan<- wire.Packet) error {
	env.logger.Info("Initiating open request")

	var p wire.Packet
	var err error

	// generate a new offset for the handshake.
	offset := uint64(rand.Uint32())

	// Send: open
	if err = sendOrTimeout(env, out, wire.BuildPacket(route).SetOpen(offset).Build()); err != nil {
		return NewOpeningError(fmt.Sprintf("Failed: send(open): %v", err.Error()))
	}

	// Receive: open, verify
	p, err = recvOrTimeout(env, in)
	if err != nil || p == nil {
		ret := NewOpeningError(fmt.Sprintf("Failed: recv(open, verify): %v", err.Error()))
		sendOrTimeout(env, out, wire.BuildPacket(route).SetError(ret).Build())
		return ret
	}

	open, verify := p.Open(), p.Verify()
	if open == nil || verify == nil {
		ret := NewOpeningError("Failed: recv(open, verify): Missing open or verify")
		sendOrTimeout(env, out, wire.BuildPacket(route).SetError(ret).Build())
		return ret
	}

	if verify.Val() != offset {
		ret := NewOpeningError(fmt.Sprintf("Failed: receive(open, verify): Incorrect verify [%v]", verify.Val()))
		sendOrTimeout(env, out, wire.BuildPacket(route).SetError(ret).Build())
		return ret
	}

	// Send: verify
	if err = sendOrTimeout(env, out, wire.BuildPacket(route).SetVerify(open.Val()).Build()); err != nil {
		ret := NewOpeningError(fmt.Sprintf("Failed: send(verify): %v", err.Error()))
		sendOrTimeout(env, out, wire.BuildPacket(route).SetError(ret).Build())
		return ret
	}

	return nil
}

// Performs receiver (ie listener) open handshake: recv(open), send(open,verify), recv(verify)
func openRecv(route wire.Route, env *tunnelEnv, in <-chan wire.Packet, out chan<- wire.Packet) error {
	logger := env.logger

	logger.Info("Waiting for open request")

	// Receive: open
	logger.Info("Receive (open)")
	p, err := recvOrTimeout(env, in)
	if err != nil {
		return NewOpeningError(fmt.Sprintf("Failed: recv(open): %v", err))
	}

	open := p.Open()
	if open == nil {
		ret := NewOpeningError("Failed: recv(open): Missing open message.")
		sendOrTimeout(env, out, wire.BuildPacket(route).SetError(ret).Build())
		return ret
	}

	// Send: open,verify
	logger.Debug("Sending (open,verify)")
	offset := uint64(rand.Uint32())
	if err := sendOrTimeout(env, out, wire.BuildPacket(route).SetVerify(open.Val()).SetOpen(offset).Build()); err != nil {
		ret := NewOpeningError(fmt.Sprint("Failed: send(open,verify): %v", err))
		sendOrTimeout(env, out, wire.BuildPacket(route).SetError(ret).Build())
		return ret
	}

	// Receive: verify
	logger.Debug("Receive (verify)")
	p, err = recvOrTimeout(env, in)
	if err != nil {
		ret := NewOpeningError(fmt.Sprint("Failed: recv(verify): %v", err))
		sendOrTimeout(env, out, wire.BuildPacket(route).SetError(ret).Build())
		return ret
	}

	verify := p.Verify()
	if verify == nil || verify.Val() != offset {
		ret := NewOpeningError("Failed: recv(verify): Missing or incorrect verify")
		sendOrTimeout(env, out, wire.BuildPacket(route).SetError(ret).Build())
		return ret
	}

	return nil
}

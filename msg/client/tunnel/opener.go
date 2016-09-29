package tunnel

import (
	"fmt"
	"math/rand"

	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
)

// TODO: Due to quick refactor, these workers don't respond to controller signals yet.
func NewOpenerInit(env *Env, in <-chan wire.Packet, out chan<- wire.Packet) func(utils.StateController, []interface{}) {
	return func(state utils.StateController, args []interface{}) {
		var err error

		for i := 0; i < env.config.MaxRetries; i++ {
			if err = openInit(env, in, out); err == nil {
				state.Next(TunnelOpened)
				return
			}
		}

		state.Fail(err)
	}
}

func NewOpenerRecv(env *Env, in <-chan wire.Packet, out chan<- wire.Packet) func(utils.StateController, []interface{}) {
	return func(state utils.StateController, args []interface{}) {
		var err error

		for i := 0; i < env.config.MaxRetries; i++ {
			if err = openRecv(env, in, out); err == nil {
				state.Next(TunnelOpened)
				return
			}
		}

		state.Fail(err)
	}
}

// Performs open handshake: send(open), recv(open,verify), send(verify)
func openInit(env *Env, in <-chan wire.Packet, out chan<- wire.Packet) error {
	env.Log("Initiating open request")

	var p wire.Packet
	var err error

	// generate a new offset for the handshake.
	offset := uint64(rand.Uint32())

	// Send: open
	if err = env.sendOrTimeout(out, wire.BuildPacket(env.route).SetOpen(offset).Build()); err != nil {
		return NewOpeningError(fmt.Sprintf("Failed: send(open): %v", err.Error()))
	}

	// Receive: open, verify
	p, err = env.recvOrTimeout(in)
	if err != nil || p == nil {
		ret := NewOpeningError(fmt.Sprintf("Failed: recv(open, verify): %v", err.Error()))
		env.sendOrTimeout(out, wire.BuildPacket(env.route).SetError(ret).Build())
		return ret
	}

	open, verify := p.Open(), p.Verify()
	if open == nil || verify == nil {
		ret := NewOpeningError("Failed: recv(open, verify): Missing open or verify")
		env.sendOrTimeout(out, wire.BuildPacket(env.route).SetError(ret).Build())
		return ret
	}

	if verify.Val() != offset {
		ret := NewOpeningError(fmt.Sprintf("Failed: receive(open, verify): Incorrect verify [%v]", verify.Val()))
		env.sendOrTimeout(out, wire.BuildPacket(env.route).SetError(ret).Build())
		return ret
	}

	// Send: verify
	if err = env.sendOrTimeout(out, wire.BuildPacket(env.route).SetVerify(open.Val()).Build()); err != nil {
		ret := NewOpeningError(fmt.Sprintf("Failed: send(verify): %v", err.Error()))
		env.sendOrTimeout(out, wire.BuildPacket(env.route).SetError(ret).Build())
		return ret
	}

	return nil
}

// Performs receiver (ie listener) open handshake: recv(open), send(open,verify), recv(verify)
func openRecv(env *Env, in <-chan wire.Packet, out chan<- wire.Packet) error {
	env.Log("Waiting for open request")

	// Receive: open
	env.Log("Receive (open)")
	p, err := env.recvOrTimeout(in)
	if err != nil {
		return NewOpeningError(fmt.Sprintf("Failed: recv(open): %v", err))
	}

	open := p.Open()
	if open == nil {
		ret := NewOpeningError("Failed: recv(open): Missing open message.")
		env.sendOrTimeout(out, wire.BuildPacket(env.route).SetError(ret).Build())
		return ret
	}

	// Send: open,verify
	env.Log("Sending (open,verify)")
	offset := uint64(rand.Uint32())
	if err := env.sendOrTimeout(out, wire.BuildPacket(env.route).SetVerify(open.Val()).SetOpen(offset).Build()); err != nil {
		ret := NewOpeningError(fmt.Sprint("Failed: send(open,verify): %v", err))
		env.sendOrTimeout(out, wire.BuildPacket(env.route).SetError(ret).Build())
		return ret
	}

	// Receive: verify
	env.Log("Receive (verify)")
	p, err = env.recvOrTimeout(in)
	if err != nil {
		ret := NewOpeningError(fmt.Sprint("Failed: recv(verify): %v", err))
		env.sendOrTimeout(out, wire.BuildPacket(env.route).SetError(ret).Build())
		return ret
	}

	verify := p.Verify()
	if verify == nil || verify.Val() != offset {
		ret := NewOpeningError("Failed: recv(verify): Missing or incorrect verify")
		env.sendOrTimeout(out, wire.BuildPacket(env.route).SetError(ret).Build())
		return ret
	}

	return nil
}

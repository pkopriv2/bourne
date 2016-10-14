package client

import (
	"fmt"
	"io"

	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/machine"
	"github.com/pkopriv2/bourne/message/client/tunnel"
	"github.com/pkopriv2/bourne/message/wire"
)

// Much of this was inspired by the following papers:
//
// https://tools.ietf.org/html/rfc793
// http://www.ietf.org/proceedings/44/I-D/draft-ietf-sigtran-reliable-udp-00.txt
// https://tools.ietf.org/html/rfc4987
//
// TODOS:
//   * Make open/close more reliable.
//   * Make open/close more secure.  (Ensure packets are properly addressed!)
//   * Close streams independently!
//   * Protect against offset flood.
//   * Drop packets when recv buffer is full!!!
//   * Better concurrency model!
//

// An active tunnel represents one side of a conversation between two entities.
//
//   * A tunnel reprents a full-duplex stream abstraction.
//
//   * A tunnel may be thought of as two send streams, with one sender on each
//     side of the session.
//
//   * Each sender is responsible for the reliability his data stream.
//
//   * Senders currently implement timeout based retransmission.
//
//  Similar to TCP, this protocol ensures reliable, in-order delivery of data.
//  This also allows sessions to be resumed if errors occur. However, unlike
//  TCP, this does NOT attempt to solve the following problems:
//
//     * Flow/congestion control
//     * Message integrity
//
//  The class implements the following state machine:
//
//
//          |-->openRecv--|           |-->closeRecv--|
//    init--|             |-->opened--|              |-->closed
//          |-->openInit--|           |-->closeInit--|
//
//
//
// Each state implements a separate "machine" or alternatively, a separate
// communications network.
//
//
// Tunnel Opening:
//     1. Transition to Opening.
//     2. Perform reliable handshake
//         * Initiator: send(open), recv(open,verify), send(verify)
//         * Listener:  recv(open), send(open,verify), recv(verify)
//     3. Transition to Opened.
//
// Tunnel Closing:
//     1. Transition to Closing
//     2. Perform close handshake
//         * Initiator: send(close), recv(close,verify), send(verify)
//         * Listener:  recv(close), send(close,verify), recv(verify)
//     3. Transition to Closed.
//
func NewTunnelClosedError(reason error) error {
	return fmt.Errorf("TUNNEL:CLOSED[error=%v]", reason)
}

type Tunnel interface {
	io.Closer
	io.Reader
	io.Writer

	Route() wire.Route
}

// the main tunnel abstraction
type tunnelImpl struct {
	socket  TunnelSocket
	machine machine.StateMachine

	streamRx *concurrent.Stream
	streamTx *concurrent.Stream

	close  chan struct{}
	closed chan struct{}
}

func NewTunnel(socket TunnelSocket) Tunnel {
	config := socket.Context().Config()

	rx := concurrent.NewStream(1 << 20)
	tx := concurrent.NewStream(1 << 20)

	network := tunnel.NewTunnelNetwork(config, socket)

	// // opening workers
	openerInit := tunnel.NewOpenerInit(socket.Route(), socket.Context(), network.MainSocket())
	openerRecv := tunnel.NewOpenerRecv(socket.Route(), socket.Context(), network.MainSocket())

	// // closing workers
	closerInit := tunnel.NewCloserInit(socket.Route(), socket.Context(), network.MainSocket())
	closerRecv := tunnel.NewCloserRecv(socket.Route(), socket.Context(), network.MainSocket())

	// opened workers
	sender := tunnel.NewSender(socket.Route(), socket.Context(), tx, network.SenderSocket())
	receiver := tunnel.NewReceiver(socket.Context(), network.ReceiverSocket())
	assembler := tunnel.NewAssembler(socket.Context(), network.AssemblerSocket())
	bufferer := tunnel.NewBufferer(socket.Context(), network.BuffererSocket(), rx)
	verifier := tunnel.NewVerifier(socket.Route(), socket.Context(), network.VerifierSocket())

	// build the machine
	builder := machine.NewStateMachine()
	builder.AddState(tunnel.TunnelOpeningInit, openerInit)
	builder.AddState(tunnel.TunnelOpeningRecv, openerRecv)
	builder.AddState(tunnel.TunnelOpened, sender, verifier, receiver, assembler, bufferer)
	builder.AddState(tunnel.TunnelClosingInit, closerInit)
	builder.AddState(tunnel.TunnelClosingRecv, closerRecv)

	// start the machine
	var machine machine.StateMachine
	if socket.Listening() {
		machine = builder.Start(tunnel.TunnelOpeningRecv)
	} else {
		machine = builder.Start(tunnel.TunnelOpeningInit)
	}

	t := &tunnelImpl{
		machine:  machine,
		socket:   socket,
		closed:   make(chan struct{}),
		close:    make(chan struct{}),
		streamRx: rx,
		streamTx: tx}
	go controlTunnel(t)
	return t
}

func controlTunnel(t *tunnelImpl) {
	defer t.socket.Done()

	select {
	case <-t.close:
		t.machine.Move(tunnel.TunnelClosingInit)
	case <-t.socket.Closed():
		t.machine.Move(tunnel.TunnelClosingInit)
	case <-t.socket.Failed():
		t.machine.Fail(t.socket.Failure())
	}

	t.machine.Wait()
	t.streamRx.Close()
	t.streamTx.Close()
	close(t.closed)
}

func (t *tunnelImpl) Route() wire.Route {
	return t.socket.Route()
}

func (t *tunnelImpl) Read(p []byte) (int, error) {
	n, err := t.streamRx.Read(p)
	if err == concurrent.ErrStreamClosed {
		return 0, NewTunnelClosedError(t.machine.Result())
	}

	return n, err
}

func (t *tunnelImpl) Write(p []byte) (int, error) {
	n, err := t.streamTx.Write(p)
	if err == concurrent.ErrStreamClosed {
		return 0, NewTunnelClosedError(t.machine.Result())
	}

	return n, err
}

func (t *tunnelImpl) Close() error {
	select {
	case <-t.closed:
		return NewTunnelClosedError(t.machine.Result())
	case t.close <- struct{}{}:
	}

	<-t.closed
	return t.machine.Result()
}

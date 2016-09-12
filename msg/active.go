package msg

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"math/rand"

	"github.com/emirpasic/gods/maps/treemap"
)

// Much of this was inspired by the following papers:
//
// https://tools.ietf.org/html/rfc793
// http://www.ietf.org/proceedings/44/I-D/draft-ietf-sigtran-reliable-udp-00.txt
//
var (
	ErrChannelClosed   = errors.New("CHAN:CLOSED")
	ErrChannelError    = errors.New("CHAN:ERROR")
	ErrHandshakeFailed = errors.New("CHAN:HANDSHAKE")
	ErrResponse        = errors.New("CHAN:BADRESPONSE")
	ErrTimeout         = errors.New("CHAN:TIMEOUT")
	ErrInvalidState    = errors.New("CHAN:INVALIDSTATE")
)

// channel states
const (
	ChannelOpening AtomicState = 1 << iota
	ChannelOpened
	ChannelClosing
	ChannelClosed
	ChannelError
)

// An active channel represents one side of a conversation between two entities.
//
// The majority of packet stream logic is handled here. The fundamental laws of
// channels are as follows:
//
//   * A channel reprents a full-duplex stream abstraction.  In other words,
//     there are independent input and output streams.  Read again: INDEPENDENT!
//
//   * A channel may be thought of as two send streams, with one sender on each
//     side of the conversation.
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
//    init-->opening-->opened-->closing-->closed
//              |         |        |
//	            |--------->-------->----->error
//
//  The state machine is implemented using compare and swaps.
//  In other words, there is no guarantee of strong consistency
//  with respect to the actions taken with out of date knowledge.
//  This is done by design, as the need for strong consistency
//  lead to deadlock where an external action block transitions
//  indefinitely if they themselves are blocked or long running.
//  Consistency errors are avoided by synchronized access to any
//  shared data structures.
//
//  This class uses the following threading model:
//
//  thread1: (consumer read thread)
//     * Makes external facing calls (e.g. Read/Close)
//  thread2: (consumer write thread) [optional]
//     * Makes external facing calls (e.g. Write/Flush/Close)
//  thread3: (mux router thread)
//     * Spawns and routes packets to the thread.  Can also initiate a close.
//  thread4: (recv thread)
//     * Accepts incoming data from a different channel and pushes it to consumer thread.
//  thread5: (send thread)
//     * Accepts incoming data from the consumer and pushes it to the target channel.
//  thread6: (opening thread)
//     * Performs the opening handshake.  This is a shortlived thread.
//  thread7: (closing thread)
//     * Performs the closing handshake and closes all resources.  This is a shortlived thread.
//
// Channel Closing:
//
//   If initiated by consumer:
//      1. Transition the state to Closing.
//      2. Close thread is spawned (as initiator).
//      3. Close reader/writer of recvlog, and reader/writer of sendlog.  (any pending reads/writes are aborted)
//      4. Close sequence is initiated.

//   If initiated by remote channel:
//      1. Transition the state to Closing. (detected in recv thread)
//      2. Close thread is spawned (as receiver).
//      3. Close reader/writer of recvlog, and reader/writer of sendlog.  (any pending reads/writes are aborted)
//      4. Close sequence is initiated.
//
// *This object is thread safe.*
//
type ChannelActive struct {

	// the local channel address
	local ChannelAddress

	// the remote channel address
	remote ChannelAddress

	// channel options.
	options *ChannelOptions

	// general channel statistics
	stats *ChannelStats

	// the state of the channel.
	state *AtomicState

	// receive buffers
	recvIn  chan Packet
	recvLog *Stream

	// send buffers
	sendLog *Stream
	sendOut func(p *Packet) error

	// event handlers
	onInit  ChannelStateFn
	onOpen  ChannelStateFn
	onClose ChannelStateFn

	// the workers wait
	workers sync.WaitGroup
}

// Creates and returns a new channel
func NewChannelActive(l ChannelAddress, r ChannelAddress, listening bool, opts ...ChannelConfig) (*ChannelActive, error) {
	// initialize the options.
	options := DefaultChannelOptions()
	for _, opt := range opts {
		opt(options)
	}

	// create the channel
	c := &ChannelActive{
		local:   l,
		remote:  r,
		options: options,
		stats:   NewChannelStats(l),
		state:   NewAtomicState(ChannelOpening),
		recvIn:  make(chan Packet, options.RecvInSize),
		recvLog: NewStream(options.RecvLogSize),
		sendLog: NewStream(options.SendLogSize),
		sendOut: options.OnData,
		onInit:  options.OnInit,
		onOpen:  options.OnOpen,
		onClose: options.OnClose}

	// call the init function.
	c.onInit(c)

	// kick off the workers
	c.workers.Add(3)
	go sendWorker(c)
	go recvWorker(c)
	go openWorker(c, listening)

	// finally, return it.
	return c, nil
}

// Returns the local address of this channel
func (c *ChannelActive) LocalAddr() ChannelAddress {
	return c.local
}

// Returns the remote address of this channel
func (c *ChannelActive) RemoteAddr() ChannelAddress {
	return c.remote
}

func (c *ChannelActive) Flush() error {
	return c.flush(365 * 24 * time.Hour)
}

// Reads data from the channel.  Blocks if data isn't available.
func (c *ChannelActive) Read(buf []byte) (int, error) {
	state := c.state.WaitUntil(ChannelOpened | ChannelClosed | ChannelError)
	if state.Is(ChannelClosed|ChannelError) {
		return 0, ErrChannelClosed
	}

	return c.recvLog.Read(buf)
}

// Writes the data to the channel.  Blocks if the underlying send buffer is full.
func (c *ChannelActive) Write(data []byte) (int, error) {
	state := c.state.WaitUntil(ChannelOpened | ChannelClosed | ChannelError)
	if state.Is(ChannelClosed|ChannelError) {
		return 0, ErrChannelClosed
	}

	return c.sendLog.Write(data)
}

// Closes the channel.  Returns an error if the channel is already closed.
func (c *ChannelActive) Close() error {
	state := c.state.WaitUntil(ChannelOpened | ChannelClosed | ChannelError)
	if state.Is(ChannelClosed|ChannelError) {
		return ErrChannelClosed
	}

	if ! c.state.Transition(ChannelOpened, ChannelClosing) {
		return ErrChannelClosed
	}

	c.workers.Add(1)
	go closeWorker(c, true)
	c.workers.Wait()
	return nil
}

// ** INTERNAL ONLY METHODS **

// Send pushes a message on the input channel.  (used for internal routing.)
func (c *ChannelActive) send(p *Packet) error {
	if ! c.state.Is(ChannelOpening | ChannelOpened | ChannelClosing) {
		return ErrChannelClosed
	}

	c.recvIn <- *p
	return nil
}

// Flushes the sendlog.
func (c *ChannelActive) flush(timeout time.Duration) error {
	state := c.state.WaitUntil(ChannelOpened | ChannelClosed | ChannelError)
	if state.Is(ChannelClosed) {
		return ErrChannelClosed
	}
	if state.Is(ChannelError) {
		return ErrChannelError
	}

	tail, _, head, closed := c.sendLog.Refs()
	if closed {
		return ErrChannelClosed
	}

	if tail.offset < head.offset {
		return nil
	}

	start := time.Now()
	for {
		if time.Since(start) >= timeout {
			return ErrTimeout
		}

		time.Sleep(c.options.SendWait)
		tail, _, _, closed = c.sendLog.Refs()
		if closed {
			return ErrChannelClosed
		}
	}
}

// Logs a message, tagging it with the channel's local address.
func (c *ChannelActive) log(format string, vals ...interface{}) {
	if !c.options.Debug {
		return
	}

	log.Println(fmt.Sprintf("[%v] -- ", c.local) + fmt.Sprintf(format, vals...))
}

func openWorker(c *ChannelActive, listening bool) {
	defer c.workers.Done()

	var err error
	if listening {
		err = openRecv(c)
	} else {
		err = openInit(c)
	}

	if err != nil {
		c.state.Transition(ChannelOpening, ChannelError)
	} else {
		c.state.Transition(ChannelOpening, ChannelOpened)
	}
}

func closeWorker(c *ChannelActive, initiator bool) {
	defer c.workers.Done()

	var err error
	if initiator {
		err = closeInit(c)
	} else {
		err = closeRecv(c)
	}

	c.sendLog.Close()
	c.recvLog.Close()

	if err != nil {
		c.state.Transition(AnyAtomicState, ChannelError)
	} else {
		c.state.Transition(ChannelClosing, ChannelClosed)
	}
}

func sendWorker(c *ChannelActive) {
	defer c.workers.Done()
	defer c.log("Send worker shutdown")

	// initialize the timeout values
	timeout := c.options.AckTimeout
	timeoutCnt := 0

	// track last ack received
	recvAck, _, _, _ := c.sendLog.Refs()

	// track last ack sent
	_, _, sendAck, _ := c.recvLog.Refs()

	// the packet buffer (initialized here so we don't continually recreate memory.)
	tmp := make([]byte, PacketMaxLength)
	for {
		state := c.state.WaitUntil(ChannelOpened | ChannelClosing | ChannelClosed | ChannelError)
		if ! state.Is(ChannelOpened) {
			return
		}

		var err error

		// ** IMPORTANT ** Channel state can still change!  Need to lock
		// at places that can have external side effects, or at least be
		// able to detect state changes and handle appropriately.

		// let's see if we need to retransmit
		sendTail, sendCur, _, sendClosed := c.sendLog.Refs()
		if sendClosed {
			c.log("Send log closed")
			return
		}

		// if we received an ack recently, reset the timeout values
		if sendTail.offset > recvAck.offset {
			recvAck = sendTail
			timeout = c.options.AckTimeout
			timeoutCnt = 0
		}

		// let's see if we're in a timeout senario.
		if sendCur.offset > sendTail.offset && time.Since(sendTail.time) >= timeout {
			cur, prev, err := c.sendLog.Reset()
			if err != nil {
				c.log("Send log closed")
				return
			}

			c.stats.numResets.Inc(1)
			c.log("Ack timed out. Reset send log to [%v] from [%v]", cur.offset, prev.offset)

			// double the timeout (ie exponential backoff)
			timeout *= 2
			c.log("Ack timeout increased to [%v]", timeout)

			if timeoutCnt++; timeoutCnt >= 3 {
				c.state.Transition(AnyAtomicState, ChannelError)
				return
			}
		}

		// stop sending if we're timing out.
		if timeoutCnt > 0 {
			time.Sleep(c.options.SendWait)
			continue
		}

		// start building the outgoing packet
		flags := PacketFlagNone

		// see if we should be sending a data packet.
		sendStart, num, err := c.sendLog.TryRead(tmp, false)
		if err != nil {
			return
		}

		// build the packet data.
		data := tmp[:num]
		if num > 0 {
			flags = flags | PacketFlagOffset
		}

		// see if we should be sending an ack.
		_, _, recvHead, _ := c.recvLog.Refs()
		if recvHead.offset > sendAck.offset {
			flags = flags | PacketFlagAck
			sendAck = recvHead
		}

		// just sleep if nothing to do
		if flags == PacketFlagNone {
			time.Sleep(c.options.SendWait)
			continue
		}

		// this can block indefinitely (What should we do???)
		if err := c.sendOut(newPacket(c, flags, sendStart.offset, recvHead.offset, data)); err != nil {
			c.state.Transition(AnyAtomicState, ChannelError)
			return
		}

		c.stats.packetsSent.Inc(1)
	}
}

func recvWorker(c *ChannelActive) {
	defer c.workers.Done()
	defer c.log("Recv worker shutdown")

	// we'll use a simple sorted tree map to track out of order segments
	pending := treemap.NewWith(OffsetComparator)
	for {
		// block until we can do something useful!
		state := c.state.WaitUntil(ChannelOpened | ChannelClosing | ChannelClosed | ChannelError)
		if ! state.Is(ChannelOpened) {
			return
		}

		// ** IMPORTANT ** Channel state can still change!  Need to lock
		// at places that can have external side effects, or at least be
		// able to detect state changes and handle appropriately.

		// grab the next packet (cannot block as we need to evaluate state transitions)
		var p *Packet = nil
		select {
		case in, ok := <-c.recvIn:
			if !ok {
				return
			}

			p = &in
		default:
			break
		}

		if p == nil {
			time.Sleep(c.options.RecvWait)
			continue
		}

		c.stats.packetsReceived.Inc(1)

		// Handle: close flag
		if p.ctrls&PacketFlagClose > 0 {
			if ! c.state.Transition(ChannelOpened, ChannelClosing) {
				return
			}

			c.workers.Add(1)
			go closeWorker(c, false)
			return
		}

		// Handle: ack flag
		if p.ctrls&PacketFlagAck > 0 {
			_, err := c.sendLog.Commit(p.ack)
			switch err {
			case ErrStreamClosed:
				c.log("Error committing ack. Send log closed.")
				return
			case ErrStreamInvalidCommit:
				c.log("Error committing ack [%v] : [%v]", p.ack, err)
				c.stats.packetsDropped.Inc(1)
				continue
			}
		}

		// Handle: data flag (consume elements of the stream)
		if p.ctrls&PacketFlagOffset > 0 {
			pending.Put(p.offset, p.data)
		}

		// consume the pending items
		for {
			k, v := pending.Min()
			if k == nil || v == nil {
				break
			}

			// Take a snapshot of the current receive stream offsets
			_, _, head, _:= c.recvLog.Refs()

			// Handle: Future offset
			offset, data := k.(uint32), v.([]byte)
			if offset > head.offset {
				break
			}

			// Handle: Past offset
			pending.Remove(offset)
			if head.offset > offset+uint32(len(data)) {
				continue
			}

			// Handle: Write the valid elements of the segment.
			if _, err := c.recvLog.Write(data[head.offset-offset:]); err != nil {
				return
			}
		}
	}
}

// Performs initiator  open handshake: send(open), recv(open,ack), send(ack)
func openInit(c *ChannelActive) error {

	var p *Packet
	var err error

	// generate a new offset for the handshake.
	offset := rand.Uint32()

	// Send: open
	if err = sendHeader(c, PacketFlagOpen, offset, 0); err != nil {
		return ErrHandshakeFailed
	}

	// Receive: open, ack
	p, err = recvOrTimeout(c, c.options.AckTimeout)
	if err != nil || p == nil {
		sendHeader(c, PacketFlagErr, 0, 0)
		return ErrTimeout
	}

	if p.ctrls != (PacketFlagOpen|PacketFlagAck) || p.ack != offset {
		sendHeader(c, PacketFlagErr, 0, 0)
		return ErrHandshakeFailed
	}

	// Send: ack
	if err = sendHeader(c, PacketFlagAck, 0, p.offset); err != nil {
		return ErrHandshakeFailed
	}

	return nil
}

// Performs receiver (ie listener) open handshake: recv(open), send(open,ack), recv(ack)
func openRecv(c *ChannelActive) error {
	// Receive: open (wait indefinitely)
	in, ok := <-c.recvIn
	if !ok {
		return ErrHandshakeFailed
	}

	var p = &in

	if p.ctrls != PacketFlagOpen {
		sendHeader(c, PacketFlagErr, 0, 0)
		return ErrHandshakeFailed
	}

	// Send: open,ack
	offset := rand.Uint32()
	if err := sendHeader(c, PacketFlagOpen|PacketFlagAck, offset, p.offset); err != nil {
		return ErrHandshakeFailed
	}

	// Receive: Ack
	p, err := recvOrTimeout(c, c.options.AckTimeout)
	if err != nil {
		sendHeader(c, PacketFlagErr, 0, 0)
		return ErrHandshakeFailed
	}

	if p.ctrls != PacketFlagAck || p.ack != offset {
		sendHeader(c, PacketFlagErr, 0, 0)
		return ErrHandshakeFailed
	}

	return nil
}

// Performs initiator  open handshake: send(close), recv(close, ack), send(ack)
func closeInit(c *ChannelActive) error {
//
	// var p *Packet
	// var err error
//
	// // generate a new offset for the handshake.
	// offset := rand.Uint32()
//
	// // Send: open
	// if err = sendHeader(c, PacketFlagClose, offset, 0); err != nil {
		// return ErrHandshakeFailed
	// }
//
	// // Receive: open, ack
	// p, err = recvOrTimeout(c, c.options.AckTimeout)
	// if err != nil || p == nil {
		// sendHeader(c, PacketFlagErr, 0, 0)
		// return ErrTimeout
	// }
//
	// if p.ctrls != (PacketFlagOpen|PacketFlagAck) || p.ack != offset {
		// sendHeader(c, PacketFlagErr, 0, 0)
		// return ErrHandshakeFailed
	// }
//
	// // Send: ack
	// if err = sendHeader(c, PacketFlagAck, 0, p.offset); err != nil {
		// return ErrHandshakeFailed
	// }

	return nil
}

func closeRecv(c *ChannelActive) error {
	return nil
}

func newPacket(c *ChannelActive, flags PacketFlags, offset uint32, ack uint32, data []byte) *Packet {
	return NewPacket(c.local.entityId, c.local.channelId, c.remote.entityId, c.remote.channelId, flags, offset, ack, 0, data)
}

func sendHeader(c *ChannelActive, flags PacketFlags, offset uint32, ack uint32) error {
	return c.sendOut(newPacket(c, flags, offset, ack, []byte{}))
}

func recvOrTimeout(c *ChannelActive, timeout time.Duration) (*Packet, error) {
	timer := time.NewTimer(timeout)

	select {
	case <-timer.C:
		return nil, ErrTimeout
	case p := <-c.recvIn:
		return &p, nil
	}
}

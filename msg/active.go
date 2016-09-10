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
	ErrHandshakeFailed = errors.New("CHAN:HANDSHAKE")
	ErrResponse        = errors.New("CHAN:BADRESPONSE")
	ErrTimeout         = errors.New("CHAN:TIMEOUT")
	ErrInvalidState    = errors.New("CHAN:INVALIDSTATE")
)

// channel states
const (
	ChannelOpening State = 1 << iota
	ChannelOpened
	ChannelClosing
	ChannelClosed
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
//  Send Data Flow:
//
//     <consumer> ---> [ ring buffer ] ---> sender ---> out
//
//  Receiver
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
	state *StateMachine

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
		state:   NewStateMachine(ChannelOpening),
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

// Reads data from the channel.  Blocks if data isn't available.
func (c *ChannelActive) Read(buf []byte) (int, error) {
	c.state.WaitUntil(ChannelOpened | ChannelClosed)

	var num int
	var err error

	num, err = 0, ErrChannelClosed
	c.state.ApplyIf(ChannelOpened, func() {
		num, err = c.recvLog.Read(buf)
	})

	return num, err
}

// Writes the data to the channel.  Blocks if the underlying send buffer is full.
func (c *ChannelActive) Write(data []byte) (int, error) {
	c.state.WaitUntil(ChannelOpened | ChannelClosed)

	var num int
	var err error

	num, err = 0, ErrChannelClosed
	c.state.ApplyIf(ChannelOpened, func() {
		num, err = c.sendLog.Write(data)
	})

	return num, err
}

// Closes the channel.  Returns an error if the channel is already closed.
// TODO: Better close semantics.  (ie what happens to data in the out buffer, etc..)
func (c *ChannelActive) Close() error {
	c.state.WaitUntil(ChannelOpened | ChannelClosed)

	_, err := c.state.Transition(ChannelOpened, func() State {
		return ChannelClosed
	})

	if err != nil {
		return err
	}

	// wait on the workers
	c.workers.Wait()
	return nil
}

// ** INTERNAL ONLY METHODS **

// Send pushes a message on the input channel.
func (c *ChannelActive) send(p *Packet) error {
	_, err := c.state.ApplyIf(ChannelOpening|ChannelOpened|ChannelClosing, func() {
		c.recvIn <- *p
	})

	if err != nil {
		return ErrChannelClosed
	} else {
		return nil
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
		c.log("Error opening channel: %v", err)
		c.state.Transition(ChannelOpening, To(ChannelClosed))
	} else {
		c.state.Transition(ChannelOpening, To(ChannelOpened))
	}
}

func sendWorker(c *ChannelActive) {
	defer c.workers.Done()

	// initialize the timeout values
	// timeout := c.options.AckTimeout
	// timeoutCnt := 0
	//
	// // track last ack received
	// recvack, _, _ := c.sendlog.refs()

	// track last ack sent
	_, _, sendAck := c.recvLog.Refs()

	// the packet buffer (initialized here so we don't continually recreate memory.)
	tmp := make([]byte, PacketMaxLength)
	for {
		// give exclusive access to recv while waiting to open.
		state := c.state.WaitUntil(ChannelOpened | ChannelClosed)
		if state != ChannelOpened {
			return
		}

		// ** IMPORTANT ** Channel state can still change!  Need to lock
		// at places that can have external side effects, or at least be
		// able to detect state changes and handle appropriately.

		// let's see if we need to retransmit
		// sendTail, _, _ := c.sendLog.Refs()

		// if we received an ack recently, reset the timeout counters
		// if sendTail.offset > recvAck.offset {
		// recvAck = sendTail
		// timeoutCur = timeoutInit
		// timeoutCnt = 0
		// }

		// // let's see if we're in a timeout senario.
		// if time.Since(sendTail.time) >= timeoutCur {
		// cur, prev := c.sendLog.Reset()
		// c.Log("Ack timed out. Reset send log to [%v] from [%v]", cur.offset, prev.offset)
		//
		// timeoutCur *= 2
		// c.Log("Increasing ack timeout to [%v]", timeoutCur)
		//
		// if timeoutCnt += 1; timeoutCnt > 3 {
		// c.Log("Max timeouts reached")
		// return
		// }
		// }
		//
		// // stop sending if we're timing out.
		// if timeoutCnt > 0 {
		// c.Log("Currently waiting for acks before sending data")
		// time.Sleep(c.options.SendWait)
		// continue
		// }

		// start building the outgoing packet
		flags := PacketFlagNone

		// see if we should be sending a data packet.
		sendStart, num := c.sendLog.TryRead(tmp, false)

		// build the packet data.
		data := tmp[:num]
		if num > 0 {
			flags = flags | PacketFlagData
		}

		// see if we should be sending an ack.
		_, _, recvHead := c.recvLog.Refs()
		if recvHead.offset > sendAck.offset {
			flags = flags | PacketFlagAck
			sendAck = recvHead
		}

		// just sleep if nothing to do
		if flags == PacketFlagNone {
			time.Sleep(c.options.SendWait)
			continue
		}

		// finally, send the packet.
		_, err := c.state.ApplyIf(ChannelOpened, func() {
			c.sendOut(newPacket(c, flags, sendStart.offset, recvHead.offset, data))
		})

		if err != nil {
			c.log("Closing send worker.")
			return
		}

		c.stats.packetsSent.Inc(1)
	}
}

func recvWorker(c *ChannelActive) {
	defer c.workers.Done()

	// we'll use a simple sorted tree map to track out of order segments
	pending := treemap.NewWith(OffsetComparator)
	for {
		// block until we can do something useful!
		state := c.state.WaitUntil(ChannelOpened | ChannelClosed)
		if state == ChannelClosed {
			close(c.recvIn)
			return
		}

		// ** IMPORTANT ** Channel state can still change!  Need to lock
		// at places that can have external side effects, or at least be
		// able to detect state changes and handle appropriately.

		// grab the next packet
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
		c.log("Packet received: %v", p)

		// Handle: ack flag
		if p.ctrls&PacketFlagAck > 0 {
			_, err := c.sendLog.Commit(p.ack)
			if err != nil {
				c.log("Error committing ack [%v] : [%v]", p.ack, err)
				c.stats.packetsDropped.Inc(1)
				continue
			}
		}

		// Handle: data flag (consume elements of the stream)
		if p.ctrls&PacketFlagData > 0 {
			_, _, head := c.recvLog.Refs()

			// Handle: Past segments.  Just drop
			if head.offset > p.offset+uint32(len(p.data)) {
				c.log("Received past segment [%v, %v]", p.offset, len(p.data))
				c.stats.packetsDropped.Inc(1)
				continue
			}

			pending.Put(p.offset, p.data)
		}

		// consume the pending items
		for {
			k, v := pending.Min()
			if k == nil || v == nil {
				break
			}

			// Take a snapshot of the current receive stream offsets
			_, _, head := c.recvLog.Refs()

			// Handle: Future offset
			offset, data := k.(uint32), v.([]byte)
			if offset > head.offset {
				break
			}

			// No matter what, we're done with this segment.
			pending.Remove(offset)

			// Handle: Past offset
			if head.offset > offset+uint32(len(data)) {
				continue
			}

			// Handle: Write the valid elements of the segment.
			c.recvLog.Write(data[head.offset-offset:])
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
		return ErrResponse
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
		return ErrInvalidState
	}

	var p = &in

	if p.ctrls != PacketFlagOpen {
		sendHeader(c, PacketFlagErr, 0, 0)
		return ErrResponse
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
		return ErrTimeout
	}

	if p.ctrls != PacketFlagAck || p.ack != offset {
		sendHeader(c, PacketFlagErr, 0, 0)
		return ErrResponse
	}

	return nil
}

func closeInit(c *ChannelActive) error {
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

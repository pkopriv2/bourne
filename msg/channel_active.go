package msg

import (
	"errors"
	"fmt"
	"log"
	// "fmt"
	// "log"
	"sync"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
)

// Much of this was inspired by the following papers:
//
// https://tools.ietf.org/html/rfc793
// http://www.ietf.org/proceedings/44/I-D/draft-ietf-sigtran-reliable-udp-00.txt
//
var (
	ErrChannelClosed = errors.New("CHAN:CLOSED")
)

// channel states
const (
	ChannelInit State = iota
	ChannelOpening
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
//   * Senders currently detect drops based on timeouts.
//     (currently working to use duplicate ack detection)
//
//  Similar to TCP, this protocol ensures reliable, in-order delivery of data.
//  This also allows sessions to be resumed if errors occur. However, unlike
//  TCP, this does NOT attempt to solve the following problems:
//
//     * Flow control (we don't care if we overwhelm a recipient.  we will inherit aspects of this from TCP)
//     * Congestion control (we don't care if a network is overwhelmed)
//     * Message integrity (we won't do any integrity checking!)
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

// Creates and returns a new channel.
//
func NewChannelActive(l ChannelAddress, r ChannelAddress, opts ...ChannelConfig) (*ChannelActive, error) {
	// initialize the options.
	options := DefaultChannelOptions()
	for _, opt := range opts {
		opt(options)
	}

	state := NewStateMachine(ChannelInit)

	// create the channel
	c := &ChannelActive{
		local:   l,
		remote:  r,
		stats:   NewChannelStats(l),
		state:   state,
		options: options,
		recvIn:  make(chan Packet, options.RecvInSize),
		recvLog: NewStream(options.RecvLogSize),
		sendLog: NewStream(options.SendLogSize),
		sendOut: options.OnData,
		onInit:  options.OnInit,
		onOpen: options.OnOpen,
		onClose: options.OnClose}

	// call the init function.
	c.state.Transition(ChannelInit, func() State {
		c.onInit(c)
		return ChannelOpening
	})

	// kick off the workers
	c.workers.Add(2)
	go sendWorker(c)
	go recvWorker(c)

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
	c.state.WaitUntil(ChannelOpened, ChannelClosed)

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
	c.state.WaitUntil(ChannelOpened, ChannelClosed)

	var num int
	var err error

	num, err = 0, ErrChannelClosed
	c.state.ApplyIf(ChannelOpened, func() {
		num, err = c.sendLog.Write(data)
	})

	return num, err
}

// Sends a packet to the receive packet stream.
func (c *ChannelActive) Send(p *Packet) error {
	c.state.WaitUntil(ChannelOpened, ChannelClosed)

	err := c.state.ApplyIf(ChannelOpened, func() {
		c.recvIn <- *p
	})

	if err != nil {
		return ErrChannelClosed
	} else {
		return nil
	}

}

// Closes the channel.  Returns an error if the channel is already closed.
func (c *ChannelActive) Close() error {
	err := c.state.Transition(ChannelOpened, func() State {
		return ChannelClosing
	})

	// if error, we didn't transition.  stop now.
	if err != nil {
		return err
	}

	// give control to the workers.
	c.state.WaitUntil(ChannelClosing, ChannelClosed)

	// finally, shut it all down.
	c.workers.Wait()
	close(c.recvIn)
	return nil
}

// Logs a message, tagging it with the channel's local address.
func (c *ChannelActive) Log(format string, vals ...interface{}) {
	if !c.options.Debug {
		return
	}

	log.Println(fmt.Sprintf("[%v] -- ", c.local) + fmt.Sprintf(format, vals...))
}

func newPacket(c *ChannelActive, flags PacketFlags, offset uint32, ack uint32, data []byte) *Packet {
	return NewPacket(c.local.entityId, c.local.channelId, c.remote.entityId, c.remote.channelId, flags, offset, ack, 0, data)
}

const (
	ChannelOpeningInit State = iota
	ChannelOffsetLSent
	ChannelOffsetLAck
	ChannelOffsetRSent
	ChannelOffsetRAck
)

const (
	ChannelCloseInit State = iota
	ChannelCloseLSent
	ChannelCloseLAck
	ChannelCloseRSent
	ChannelCloseRAck
)

// impements the standard opening sequence. (this is assumed to have complete control over the channel)
func openWorker(c *ChannelActive, initiator bool) {
	// state := NewStateMachine(ChannelOpeningInit)
//
	// if initiator {
		// state.Transition(ChannelOpeningInit, func() State {
			// c.sendOut(newPacket(c, PacketFlagData, 0, 0, []byte{}))
			// return ChannelOffsetLSent
		// })
//
		// state.Transition(ChannelOffsetLSent, func() State {
			// p := <-c.recvIn
			// if ! p.ctrls&PacketFlagAck >0 {
			// }
			// return ChannelOffsetLRecv
		// })
	// }
}

// impements the standard closing sequence. (this is assumed to have complete control over the channel)
func closeWorker(c *ChannelActive, initiator bool) {
	// state := NewStateMachine(Channel)
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
		state := c.state.WaitUntil(ChannelOpened, ChannelClosed)
		if state != ChannelOpened {
			return
		}

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
		err := c.state.ApplyIf(ChannelOpened, func() {
			c.sendOut(
				NewPacket(
					c.local.entityId,
					c.local.channelId,
					c.remote.entityId,
					c.remote.channelId,
					flags,
					sendStart.offset,
					recvHead.offset,
					0,
					data))
		})

		if err != nil {
			c.Log("Closing send worker.")
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

		// before we getting
		switch c.state.Get() {
		case ChannelOpening:
			if err := c.state.Transition(ChannelOpening, func() State {
				openWorker(c, true)
				return ChannelOpened
			}); err != nil {
				panic(err)
			}

			continue
		case ChannelClosing:
			if err := c.state.Transition(ChannelClosing, func() State {
				closeWorker(c, true)
				return ChannelClosed
			}); err != nil {
				panic(err)
			}

			continue
		case ChannelClosed:
			return
		case ChannelOpened:
			break
		default:
			panic("Invalid state.")
		}

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
		c.Log("Packet received: %v", p)

		// Handle: ack flag
		if p.ctrls&PacketFlagAck > 0 {
			_, err := c.sendLog.Commit(p.ack)
			if err != nil {
				c.Log("Error committing ack [%v] : [%v]", p.ack, err)
				c.stats.packetsDropped.Inc(1)
				continue
			}
		}

		// Handle: data flag (consume elements of the stream)
		if p.ctrls&PacketFlagData > 0 {
			_, _, head := c.recvLog.Refs()

			// Handle: Past segments.  Just drop
			if head.offset > p.offset+uint32(len(p.data)) {
				c.Log("Received past segment [%v, %v]", p.offset, len(p.data))
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

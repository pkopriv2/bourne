package msg

import "io"
import "bytes"
import "encoding/binary"


// The current protocol version. This defines the basis
// for determining compatibility between changes.
//
var PROTOCOL_VERSION uint16 = 0

// The maximum data length of any packet.  One of the main
// goals of this package is to provide multiplexing over
// an io stream. In order to properly support that, we need to
// make sure that connections are efficiently shared.
// Packets must be bounded so that no individual packet
// can dominate the underlying io stream.
//
// Encoded as a uint16
//
var PACKET_MAX_DATA_LEN int = 65535

// control flags
var SEQ_FLAG uint8 = 1    // tells the recipient what sequence the message has
var ACK_FLAG uint8 = 2    // tells the recipient what sequence has been received by the sender.
var FIN_FLAG uint8 = 4    // tells the recipient to stop receiving data.
var FRC_FLAG uint8 = 8    // asks the recipient for an ACK
var ERR_FLAG uint8 = 128  // tells the recipient an unrecoverable error has occurred.

// A packet is the basic data structure defining a simple
// multiplexed data stream.
//
type Packet struct {

	// necessary for backwards/forwards compatibility
	protocolVersion uint16

	// every packet must identify its source
	srcEntityId  uint32 // TODO: move to UTF-8 String
	srcChannelId uint16

	// every packet must identify its destination
	dstEntityId  uint32 // TODO: move to UTF-8 String
	dstChannelId uint16

	// control flags
	ctrls uint8

	// reliability flags
	seq uint32
	ack uint32

	// the raw data (to be interpreted by the consumer)
	data []uint8
}

func NewReturnPacket(p *Packet, ctrls uint8, data []byte) *Packet {
	return nil
}

// Writes a packet to an io stream.  If successful,
// nil is returned.  Blocks if the writer blocks
//
func WritePacket(w io.Writer, m *Packet) error {

	// TODO:
	//    * BETTER ERRORS
	//    * VARINT ENCODING?
	//    * LENGTH PREFIXING? DELIMITING?
	if err := binary.Write(w, binary.BigEndian, &m.protocolVersion); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &m.srcEntityId); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &m.srcChannelId); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &m.dstEntityId); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &m.dstChannelId); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &m.ctrls); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &m.seq); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &m.ack); err != nil {
		return err
	}

	// write the data values
	dataLength := uint16(len(m.data))
	if err := binary.Write(w, binary.BigEndian, &dataLength); err != nil {
		return err
	}
	if _, err := w.Write(m.data); err != nil {
		return err
	}

	return nil
}

// Reads a packet from an io stream.  Any issues with the
// stream encoding will result in (nil, err) being returned.
//
func ReadPacket(r io.Reader) (*Packet, error) {
	// read all the header bytes
	headerBuf := make([]byte, 16)
	if _, err := io.ReadFull(r, headerBuf); err != nil {
		return nil, err
	}

	// read the data length bytes
	dataLenBuf := make([]byte, 2)
	if _, err := io.ReadFull(r, dataLenBuf); err != nil {
		return nil, err
	}

	// parse the length
	var dataLen uint16
	if err := binary.Read(bytes.NewReader(dataLenBuf), binary.BigEndian, &dataLen); err != nil {
		return nil, err
	}

	// read the data
	data := make([]byte, dataLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	// initialize the header reader
	headerReader := bytes.NewReader(headerBuf)

	// okay, read all the fields from the header buf and return the msg
	var protocolVersion uint16

	// addressing fields
	var srcEntityId uint32
	var srcChannelId uint16
	var dstEntityId uint32
	var dstChannelId uint16
	var ctrls uint8
	var seq uint32
	var ack uint32

	if err := binary.Read(headerReader, binary.BigEndian, &protocolVersion); err != nil {
		return nil, err
	}
	if err := binary.Read(headerReader, binary.BigEndian, &srcEntityId); err != nil {
		return nil, err
	}
	if err := binary.Read(headerReader, binary.BigEndian, &srcChannelId); err != nil {
		return nil, err
	}
	if err := binary.Read(headerReader, binary.BigEndian, &dstEntityId); err != nil {
		return nil, err
	}
	if err := binary.Read(headerReader, binary.BigEndian, &dstChannelId); err != nil {
		return nil, err
	}
	if err := binary.Read(headerReader, binary.BigEndian, &ctrls); err != nil {
		return nil, err
	}
	if err := binary.Read(headerReader, binary.BigEndian, &seq); err != nil {
		return nil, err
	}
	if err := binary.Read(headerReader, binary.BigEndian, &ack); err != nil {
		return nil, err
	}

	return &Packet{protocolVersion, srcEntityId, srcChannelId, dstEntityId, dstChannelId, ctrls, seq, ack, data}, nil
}

type PacketReader struct {
	in <-chan Packet

	tmp []byte
}

func NewPacketReader(in <-chan Packet) *PacketReader {
	return &PacketReader{in, nil}
}

func (self *PacketReader) Read(buf []byte) (int, error) {
	if self.tmp == nil {
		packet, ok := <-self.in
		if !ok {
			return 0, io.ErrClosedPipe
		}

		self.tmp = packet.data
	}

	// take a local snapshot of the temp buffer (not exactly necessary, but good practice)
	tmp := self.tmp

	// copy as much of the temp buffer as possible
	n := copy(buf, tmp)

	// if we consumed the entire temp buffer, then we're done with this packet.
	if n >= len(tmp) {
		self.tmp = nil
		return n, nil
	}

	// move the position
	self.tmp = tmp[n:]
	return n, nil
}

type PacketWriter struct {
	out chan<- []byte

	maxDataLen int
}

func NewPacketWriter(out chan<- []byte) *PacketWriter {
	return &PacketWriter{out, PACKET_MAX_DATA_LEN}
}

func (self *PacketWriter) Write(data []byte) (int, error) {

	// Because our packet structure defines an upper limit on the length
	// of data, we need to potentially split the data up into several
	// packets and send them individually
	//
	// To make this simple, just create a sliding window over the data
	//
	min := 0         // inclusive
	max := len(data) // exclusive
	end := max

	for {
		// reset max to fit within our packet limits
		if max-min > self.maxDataLen {
			max = min + self.maxDataLen + 1
		}

		// go ahead and send the array.
		self.out <- data[min:max]
		if max >= end {
			break
		}

		// move the window
		min = max
		max = end
	}

	return end, nil
}

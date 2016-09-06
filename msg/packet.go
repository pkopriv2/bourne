package msg

import "io"
import "bytes"
import "encoding/binary"

// TODO: Use VARINT encoding!  In addition to supporting 64bit numbers,
// which means the offset and start overflow no longer matter, we can also
// efficiently store them!

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
const PACKET_MAX_DATA_LEN = 65535

// control flags
var (
	PACKET_FLAG_NONE uint8 = 0
	PACKET_FLAG_DAT  uint8 = 1
	PACKET_FLAG_ACK  uint8 = 2
	PACKET_FLAG_FIN  uint8 = 4
	PACKET_FLAG_ERR  uint8 = 128
)

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

	// control values
	offset   uint32   // position of data within stream
	ack      uint32   // position of ack (i.e. start)
	capacity uint32

	// the raw data (to be interpreted by the consumer)
	data []uint8
}

func NewPacket(srcEntityId uint32, srcChannelId uint16, dstEntityId uint32, dstChannelId uint16, ctrls uint8, offset uint32, ack uint32, capacity uint32, data []byte) *Packet {
	c := make([]byte, len(data))
	copy(c, data)

	return &Packet{
		protocolVersion: PROTOCOL_VERSION,
		srcEntityId:     srcEntityId,
		srcChannelId:    srcChannelId,
		dstEntityId:     dstEntityId,
		dstChannelId:    dstChannelId,
		ctrls:           ctrls,
		offset:          offset,
		ack:             ack,
		capacity:        capacity,
		data:            c}
}

func NewReturnPacket(p *Packet, ctrls uint8, offset uint32, ack uint32, capacity uint32, data []byte) *Packet {
	return NewPacket(p.dstEntityId, p.dstChannelId, p.srcEntityId, p.srcChannelId, ctrls, offset, ack, capacity, data)
}

func NewErrorPacket(p *Packet, errorMsg string) *Packet {
	return NewReturnPacket(p, PACKET_FLAG_ERR, 0, 0, 0, []byte(errorMsg))
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
	if err := binary.Write(w, binary.BigEndian, &m.offset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &m.ack); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &m.capacity); err != nil {
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
	headerBuf := make([]byte, 27)
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
	var offset uint32
	var ack uint32
	var capacity uint32

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
	if err := binary.Read(headerReader, binary.BigEndian, &offset); err != nil {
		return nil, err
	}
	if err := binary.Read(headerReader, binary.BigEndian, &ack); err != nil {
		return nil, err
	}
	if err := binary.Read(headerReader, binary.BigEndian, &capacity); err != nil {
		return nil, err
	}

	return &Packet{protocolVersion, srcEntityId, srcChannelId, dstEntityId, dstChannelId, ctrls, offset, ack, capacity, data}, nil
}

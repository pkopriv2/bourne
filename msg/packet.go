package msg

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

// TODO:
//	* Move to varint encoding
//  * Develop compatibility scheme
//  * Support control flow

// The current protocol version. This defines the basis
// for determining compatibility between changes.
const ProtocolVersion = 0

// The maximum data length of any packet.  One of the main
// goals of this package is to provide multiplexing over
// an io stream. In order to properly support that, we need to
// make sure that connections are efficiently shared.
// Packets must be bounded so that no individual packet
// can dominate the underlying io stream.
//
// Encoded as a uint16
const PacketMaxLength = 1<<16 - 1

// Each packet will contain a sequence of control flags, indicating the nature
// of the packet and how the receiver should handle it.
type PacketFlags uint8

// Defines the possible packet flags
const (
	PacketFlagNone PacketFlags = 0
	PacketFlagOpen PacketFlags = 1 << iota
	PacketFlagOffset
	PacketFlagVerify
	PacketFlagClose
	PacketFlagErr
)

// An entity identifier is simply a number.  However, it has been explicitly
// declared so that we can move to new data types easily (e.g. UUID.).
// Clients are only concerned with equality between instances, so the
// the underlying type isn't important.
type EntityId uint32

// An endpoint represents the address to one side of a session.
type EndPoint interface {
	EntityId() EntityId
	ChannelId() uint16
}

// Creates a new endpoint and returns the value (not the reference!)
// The returned value may be used for equality checks.
func NewEndPoint(entityId EntityId, channelId uint16) EndPoint {
	return endPoint{entityId, channelId}
}

// A packet implements the wire protocol for relaying stream segements
// between channels.
//
// See: msg/channel.go for more information on the protocol details.
type packet struct {
	version uint16

	src EndPoint
	dst EndPoint

	flags PacketFlags

	offset uint32 // position of data within stream (inclusive)
	verify uint32 // position of offset of received data (exclusive)

	data []uint8
}

func NewPacket(src EndPoint, dst EndPoint, flags PacketFlags, offset uint32, verify uint32, data []byte) *packet {
	c := make([]byte, len(data))
	copy(c, data)

	return &packet{
		version: ProtocolVersion,
		src:     src,
		dst:     dst,
		flags:   flags,
		offset:  offset,
		verify:  verify,
		data:    c}
}

func NewReturnPacket(p *packet, flags PacketFlags, offset uint32, verify uint32, data []byte) *packet {
	return NewPacket(p.src, p.dst, flags, offset, verify, data)
}

func (p *packet) String() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("[%v]->[%v]  ", p.src, p.dst))

	flags := make([]string, 0, 5)

	if p.flags&PacketFlagErr > 0 {
		flags = append(flags, fmt.Sprintf("Error(%v)", string(p.data)))
	}

	if p.flags&PacketFlagOpen > 0 {
		flags = append(flags, fmt.Sprintf("Open(%v)", p.offset))
	}

	if p.flags&PacketFlagOffset > 0 {
		flags = append(flags, fmt.Sprintf("Data(%v, %v)", p.offset, len(p.data)))
	}

	if p.flags&PacketFlagVerify > 0 {
		flags = append(flags, fmt.Sprintf("Verify(%v)", p.verify))
	}

	if p.flags&PacketFlagClose > 0 {
		flags = append(flags, fmt.Sprintf("Close(%v)", p.offset))
	}

	buffer.WriteString(fmt.Sprintf("Flags: [%v] ", strings.Join(flags, "|")))
	return buffer.String()
}

// Writes a packet to an io stream.  If successful,
// nil is returned.  Blocks if the writer blocks
//
func WritePacket(w io.Writer, m *packet) error {

	// TODO:
	//    * BETTER ERRORS
	//    * VARINT ENCODING?
	//    * LENGTH PREFIXING? DELIMITING?
	srcEntityId := m.src.EntityId()
	srcChannelId := m.src.ChannelId()

	dstEntityId := m.dst.EntityId()
	dstChannelId := m.dst.ChannelId()

	if err := binary.Write(w, binary.BigEndian, &m.version); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &srcEntityId); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &srcChannelId); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &dstEntityId); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &dstChannelId); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &m.flags); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &m.offset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, &m.verify); err != nil {
		return err
	}
	// if err := binary.Write(w, binary.BigEndian, &m.window); err != nil {
	// return err
	// }

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
func ReadPacket(r io.Reader) (*packet, error) {
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
	var flags PacketFlags
	var offset uint32
	var verify uint32

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
	if err := binary.Read(headerReader, binary.BigEndian, &flags); err != nil {
		return nil, err
	}
	if err := binary.Read(headerReader, binary.BigEndian, &offset); err != nil {
		return nil, err
	}
	if err := binary.Read(headerReader, binary.BigEndian, &verify); err != nil {
		return nil, err
	}

	src := NewEndPoint(EntityId(srcEntityId), srcChannelId)
	dst := NewEndPoint(EntityId(dstEntityId), dstChannelId)

	return &packet{protocolVersion, src, dst, flags, offset, verify, data}, nil
}

type endPoint struct {
	entityId  EntityId
	channelId uint16
}

func (c endPoint) EntityId() EntityId {
	return c.entityId
}

func (c endPoint) ChannelId() uint16 {
	return c.channelId
}

func (c endPoint) String() string {
	return fmt.Sprintf("channel(%v:%v)", c.entityId, c.channelId)
}

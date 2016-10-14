package wire

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"

	uuid "github.com/satori/go.uuid"
)

// Defines the basic wire protocol for distributing streams in a non TCP/IP
// addressing environment.

const (
	PacketVersion          = 0
	PacketMaxSegmentLength = 1 << 16
)

var (
	versionId       MessageId = NewMessageId(1 << 0)
	srcMemberId     MessageId = NewMessageId(1 << 1)
	srcChannelId    MessageId = NewMessageId(1 << 2)
	dstMemberId     MessageId = NewMessageId(1 << 3)
	dstChannelId    MessageId = NewMessageId(1 << 4)
	segmentOffsetId MessageId = NewMessageId(1 << 5)
	segmentDataId   MessageId = NewMessageId(1 << 6)
	verifyId        MessageId = NewMessageId(1 << 7)
	openId          MessageId = NewMessageId(1 << 8)
	closeId         MessageId = NewMessageId(1 << 9)
	errorCodeId     MessageId = NewMessageId(1 << 10)
	errorMsgId      MessageId = NewMessageId(1 << 11)
)

type ProtocolError struct {
	code uint64
	msg  string
}

func (p *ProtocolError) Code() uint64 {
	return p.code
}

func (p *ProtocolError) Msg() string {
	return p.msg
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("ERR:PACKET[%v]:%v", e.code, e.msg)
}

func NewProtocolErrorFamily(code uint64) func(msg string) *ProtocolError {
	return func(msg string) *ProtocolError {
		return &ProtocolError{code, msg}
	}
}

var (
	NewEncodingError = NewProtocolErrorFamily(0)
)

type NumMessage interface {
	Val() uint64
}

type SegmentMessage interface {
	Offset() uint64
	Data() []byte
}

type ErrorMessage interface {
	Code() uint64
	Msg() string
}

type PacketBuilder interface {
	SetOpen(uint64) PacketBuilder
	SetClose(uint64) PacketBuilder
	SetVerify(uint64) PacketBuilder
	SetSegment(uint64, []byte) PacketBuilder
	SetError(*ProtocolError) PacketBuilder
	Build() Packet
}

type Packet interface {
	Route() Route

	Empty() bool

	Open() NumMessage
	Close() NumMessage
	Verify() NumMessage
	Segment() SegmentMessage
	Error() *ProtocolError

	Update() PacketBuilder
	Return() PacketBuilder

	Write(*bufio.Writer) error
}

func BuildPacket(route Route) PacketBuilder {
	return &packetBuilder{&packet{route: route}}
}

type packetBuilder struct {
	packet *packet
}

func (p *packetBuilder) SetOpen(val uint64) PacketBuilder {
	p.packet.open = &numMessage{val}
	return p
}

func (p *packetBuilder) SetClose(val uint64) PacketBuilder {
	p.packet.close = &numMessage{val}
	return p
}

func (p *packetBuilder) SetVerify(val uint64) PacketBuilder {
	p.packet.verify = &numMessage{val}
	return p
}

func (p *packetBuilder) SetSegment(offset uint64, data []byte) PacketBuilder {
	cop := make([]byte, len(data))
	copy(cop, data)

	p.packet.segment = &segmentMessage{offset, cop}
	return p
}

func (p *packetBuilder) SetError(err *ProtocolError) PacketBuilder {
	p.packet.err = err
	return p
}

func (p *packetBuilder) Build() Packet {
	return p.packet
}

type packet struct {
	route   Route
	open    NumMessage
	close   NumMessage
	verify  NumMessage
	segment SegmentMessage
	err     *ProtocolError
}

func (p *packet) Update() PacketBuilder {
	return &packetBuilder{&packet{p.route, p.open, p.close, p.verify, p.segment, p.err}}
}

func (p *packet) Empty() bool {
	return p.open == nil && p.close == nil && p.verify == nil && p.segment == nil && p.err == nil
}

func (p *packet) Return() PacketBuilder {
	return BuildPacket(p.route.Reverse())
}

func (p *packet) Route() Route {
	return p.route
}

func (p *packet) Open() NumMessage {
	return p.open
}

func (p *packet) Close() NumMessage {
	return p.close
}

func (p *packet) Verify() NumMessage {
	return p.verify
}

func (p *packet) Segment() SegmentMessage {
	return p.segment
}

func (p *packet) Error() *ProtocolError {
	return p.err
}

func (p *packet) Write(writer *bufio.Writer) error {
	return writePacket(writer, p)
}

func (p *packet) String() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%v ", p.route))

	msgs := make([]string, 0, 5)
	if p.open != nil {
		msgs = append(msgs, fmt.Sprintf("Open(%v)", p.open.Val()))
	}

	if p.close != nil {
		msgs = append(msgs, fmt.Sprintf("Close(%v)", p.close.Val()))
	}

	if p.verify != nil {
		msgs = append(msgs, fmt.Sprintf("Verify(%v)", p.verify.Val()))
	}

	if p.segment != nil {
		msgs = append(msgs, fmt.Sprintf("Segment(off:%v, len:%v)", p.segment.Offset(), len(p.segment.Data())))
	}

	if p.err != nil {
		msgs = append(msgs, fmt.Sprintf("Error(code:%v, msg:%v)", p.err.code, p.err.msg))
	}

	buffer.WriteString(fmt.Sprintf("Messages: [%v] ", strings.Join(msgs, "|")))
	return buffer.String()
}

type numMessage struct {
	val uint64
}

func (n *numMessage) Val() uint64 {
	return n.val
}

func NewNumMessage(i uint64) NumMessage {
	return &numMessage{i}
}

type segmentMessage struct {
	offset uint64
	data   []byte
}

func (s *segmentMessage) Offset() uint64 {
	return s.offset
}

func (s *segmentMessage) Data() []byte {
	return s.data
}

func NewSegmentMessage(offset uint64, data []byte) SegmentMessage {
	return &segmentMessage{offset, data}
}

type errorMessage struct {
	code uint64
	msg  string
}

func (e *errorMessage) Code() uint64 {
	return e.code
}

func (e *errorMessage) Msg() string {
	return e.msg
}

func writePacket(w *bufio.Writer, p *packet) error {
	parcel := NewParcel()
	// parcel.Set(versionId, p.Versio)
	parcel.Set(srcMemberId, p.route.Src().MemberId())
	parcel.Set(dstMemberId, p.route.Dst().MemberId())
	parcel.Set(srcChannelId, p.route.Src().ChannelId())
	parcel.Set(dstChannelId, p.route.Dst().ChannelId())

	if p.open != nil {
		parcel.Set(openId, p.open.Val())
	}

	if p.close != nil {
		parcel.Set(closeId, p.close.Val())
	}

	if p.verify != nil {
		parcel.Set(verifyId, p.verify.Val())
	}

	if p.segment != nil {
		parcel.Set(segmentOffsetId, p.segment.Offset())
		parcel.Set(segmentDataId, p.segment.Data())
	}

	if p.err != nil {
		parcel.Set(errorCodeId, p.err.code)
		parcel.Set(errorMsgId, p.err.msg)
	}

	err := parcel.Write(w)
	if err != nil {
		return err
	}

	return nil
}

// Reads a packet from an io stream.  Any issues with the
// stream encoding will result in (nil, err) being returned.
//
func ReadPacket(r *bufio.Reader) (Packet, error) {
	parcel, err := ReadParcel(r)
	if err != nil {
		return nil, err
	}

	var ok bool

	// version, ok := parcel.Get(versionId)
	// if !ok {
	// return nil, PacketFormatError{fmt.Sprintf("Missing version")}
	// }

	srcMemberId, ok := parcel.Get(srcMemberId)
	if !ok {
		return nil, NewEncodingError("Missing required srcMemberId")
	}

	dstMemberId, ok := parcel.Get(dstMemberId)
	if !ok {
		return nil, NewEncodingError("Missing required dstMemberId")
	}

	srcChannelId, ok := parcel.Get(srcChannelId)
	if !ok {
		return nil, NewEncodingError("Missing required srcChannelId")
	}

	dstChannelId, ok := parcel.Get(dstChannelId)
	if !ok {
		return nil, NewEncodingError("Missing required dstChannelId")
	}

	src := NewAddress(srcMemberId.(uuid.UUID), srcChannelId.(uint64))
	dst := NewAddress(dstMemberId.(uuid.UUID), dstChannelId.(uint64))

	builder := BuildPacket(NewRemoteRoute(src, dst))

	if open, ok := parcel.Get(openId); ok {
		builder.SetOpen(open.(uint64))
	}

	if close, ok := parcel.Get(closeId); ok {
		builder.SetClose(close.(uint64))
	}

	if verify, ok := parcel.Get(verifyId); ok {
		builder.SetVerify(verify.(uint64))
	}

	if offset, ok := parcel.Get(segmentOffsetId); ok {
		data, ok := parcel.Get(segmentDataId)
		if !ok {
			return nil, NewEncodingError("Segment missing data")
		}

		builder.SetSegment(offset.(uint64), data.([]byte))
	}

	if code, ok := parcel.Get(errorCodeId); ok {
		msg, ok := parcel.Get(errorMsgId)
		if !ok {
			return nil, NewEncodingError("Error missing message")
		}

		builder.SetError(&ProtocolError{code.(uint64), msg.(string)})
	}

	return builder.Build(), nil
}

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
	packetProtocolVersion = 0
	packetMaxSegmentLength   = 1 << 12
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

type PacketFormatError struct {
	msg string
}

func (e PacketFormatError) Error() string {
	return fmt.Sprintf("ERR:PACKET:FORMAT[%v]", e.msg)
}

type Open struct {
	Challenge uint64
}

type Close struct {
	Challenge uint64
}

type Segment struct {
	Offset uint64
	Data   []byte
}

type Verify struct {
	Code uint64
}

type Error struct {
	Code uint64
	Msg  string
}

type Packet struct {
	Version uint64
	Route   Route

	Open    *Open
	Close   *Close
	Segment *Segment
	Verify  *Verify
	Error   *Error
}

func NewPacket(route Route) *Packet {
	return &Packet{
		Version: packetProtocolVersion,
		Route:   route}
}

func (p *Packet) SetOpen(val uint64) {
	p.Open = &Open{val}
}

func (p *Packet) SetClose(val uint64) {
	p.Close = &Close{val}
}

func (p *Packet) SetVerify(val uint64) {
	p.Verify = &Verify{val}
}

func (p *Packet) SetSegment(offset uint64, data []byte) {
	p.Segment = &Segment{offset, data}
}

func (p *Packet) SetError(code uint64, msg string) {
	p.Error = &Error{code, msg}
}

func (p *Packet) String() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%v ", p.Route))

	msgs := make([]string, 0, 5)
	if p.Open != nil {
		msgs = append(msgs, fmt.Sprintf("Open(%v)", p.Open.Challenge))
	}

	if p.Close != nil {
		msgs = append(msgs, fmt.Sprintf("Close(%v)", p.Close.Challenge))
	}

	if p.Verify != nil {
		msgs = append(msgs, fmt.Sprintf("Close(%v)", p.Verify.Code))
	}

	if p.Segment != nil {
		msgs = append(msgs, fmt.Sprintf("Segment(%v, %v)", p.Segment.Offset, p.Segment.Data))
	}

	if p.Error != nil {
		msgs = append(msgs, fmt.Sprintf("Error(%v, %v)", p.Error.Code, p.Error.Msg))
	}

	buffer.WriteString(fmt.Sprintf("Messages: [%v] ", strings.Join(msgs, "|")))
	return buffer.String()
}

func WritePacket(w *bufio.Writer, p *Packet) error {
	parcel := NewParcel()
	parcel.Set(versionId, p.Version)
	parcel.Set(srcMemberId, p.Route.Src().MemberId())
	parcel.Set(dstMemberId, p.Route.Dst().MemberId())
	parcel.Set(srcChannelId, p.Route.Src().ChannelId())
	parcel.Set(dstChannelId, p.Route.Dst().ChannelId())

	if p.Open != nil {
		parcel.Set(openId, p.Open.Challenge)
	}

	if p.Close != nil {
		parcel.Set(closeId, p.Close.Challenge)
	}

	if p.Verify != nil {
		parcel.Set(verifyId, p.Verify.Code)
	}

	if p.Segment != nil {
		parcel.Set(segmentOffsetId, p.Segment.Offset)
		parcel.Set(segmentDataId, p.Segment.Data)
	}

	if p.Error != nil {
		parcel.Set(errorCodeId, p.Error.Code)
		parcel.Set(errorCodeId, p.Error.Code)
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
func ReadPacket(r *bufio.Reader) (*Packet, error) {
	parcel, err := ReadParcel(r)
	if err != nil {
		return nil, err
	}

	var ok bool

	version, ok := parcel.Get(versionId)
	if ! ok {
		return nil, PacketFormatError{fmt.Sprintf("Missing version")}
	}

	srcMemberId, ok := parcel.Get(srcMemberId)
	if ! ok {
		return nil, PacketFormatError{fmt.Sprintf("Missing srcMemberId")}
	}

	dstMemberId, ok := parcel.Get(dstMemberId)
	if ! ok {
		return nil, PacketFormatError{fmt.Sprintf("Missing dstMemberId")}
	}

	srcChannelId, ok := parcel.Get(srcChannelId)
	if ! ok {
		return nil, PacketFormatError{fmt.Sprintf("Missing srcChannelId")}
	}

	dstChannelId, ok := parcel.Get(dstChannelId)
	if ! ok {
		return nil, PacketFormatError{fmt.Sprintf("Missing dstChannelId")}
	}

	src := NewEndPoint(srcMemberId.(uuid.UUID), srcChannelId.(uint64))
	dst := NewEndPoint(dstMemberId.(uuid.UUID), dstChannelId.(uint64))

	packet := NewPacket(NewRemoteRoute(src, dst))
	packet.Version = version.(uint64)

	if open, ok := parcel.Get(openId); ok {
		packet.SetOpen(open.(uint64))
	}

	if close, ok := parcel.Get(closeId); ok {
		packet.SetClose(close.(uint64))
	}

	if verify, ok := parcel.Get(verifyId); ok {
		packet.SetVerify(verify.(uint64))
	}

	if offset, ok := parcel.Get(segmentOffsetId); ok {

		data, ok := parcel.Get(segmentDataId)
		if ! ok {
			return nil, PacketFormatError{fmt.Sprintf("Inconsistent data segment. Has offset but no data.")}
		}

		packet.SetSegment(offset.(uint64), data.([]byte))
	}

	if code, ok := parcel.Get(errorCodeId); ok {
		msg, _ :=  parcel.Get(errorMsgId)

		packet.SetError(code.(uint64), msg.(string))
	}

	return packet, nil
}

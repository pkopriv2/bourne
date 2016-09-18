package wire

import (
	// "bytes"

	"fmt"
	// "io"
	// "strings"

	"github.com/pkopriv2/bourne/utils"
)

// Defines the basic wire protocol for distributing streams in a non TCP/IP
// addressing environment.

// Constants
const (
	PacketMaxDataLength = 1 << 16
)

const (
	packetFormat = 0
	packetMagic  = 0x2A
)

var (
	packetReserved = []byte{}
)

// To be used when encoding errors occur.
type PacketEncodingError struct {
	reason string
}

func (e PacketEncodingError) Error() string {
	return fmt.Sprintf("Error encoding packet: ", e.reason)
}

// To be used when decoding errors occur.
type PacketDecodingError struct {
	reason string
}

func (e PacketDecodingError) Error() string {
	return fmt.Sprintf("Error decoding packet: ", e.reason)
}

// To be used when raw write errors occur.
type WriterError struct {
	err error
}

func (e WriterError) Error() string {
	return e.err.Error()
}

// To be used when raw read errors occur.
type ReaderError struct {
	err error
}

func (e ReaderError) Error() string {
	return e.err.Error()
}

// Custom types
const (
	RequestOpen  utils.BitMask = 1 << 0
	RequestClose utils.BitMask = 1 << 0

	EmptyRequest utils.BitMask = 1 << 0
	FlagNone     utils.BitMask = 0
	FlagOpen     utils.BitMask = 1
	FlagOffset   utils.BitMask = 1 << 1
	FlagVerify   utils.BitMask = 1 << 2
	FlagClose    utils.BitMask = 1 << 3
	FlagErr      utils.BitMask = 1 << 8
)

type StandardPacket struct {
	format uint64
	addr   Address
	flags  utils.BitMask
	offset uint64
	verify uint64
	data   []byte
}

// func NewStandardPacket(addr Address, flags utils.BitMask, offset uint64, verify uint64, data []byte) Packet {
// cop := make([]byte, len(data))
// copy(cop, data)
//
// return &packet{
// format: packetFormat,
// addr:   addr,
// flags:  flags,
// offset: offset,
// verify: verify,
// data:   cop}
// }
//
// func NewErrorPacket(addr Address, msg string) Packet {
// return NewStandardPacket(addr, FlagErr, 0, 0, []byte(msg))
// }
//
// func (p *packet) String() string {
// var buffer bytes.Buffer
// buffer.WriteString(fmt.Sprintf("%v  ", p.addr))
//
// flags := make([]string, 0, 5)
//
// if p.flags&FlagErr > 0 {
// flags = append(flags, fmt.Sprintf("Error(%v)", string(p.data)))
// }
//
// if p.flags&FlagOpen > 0 {
// flags = append(flags, fmt.Sprintf("Open(%v)", p.offset))
// }
//
// if p.flags&FlagOffset > 0 {
// flags = append(flags, fmt.Sprintf("Data(%v, %v)", p.offset, len(p.data)))
// }
//
// if p.flags&FlagVerify > 0 {
// flags = append(flags, fmt.Sprintf("Verify(%v)", p.verify))
// }
//
// if p.flags&FlagClose > 0 {
// flags = append(flags, fmt.Sprintf("Close(%v)", p.offset))
// }
//
// buffer.WriteString(fmt.Sprintf("utils.BitMask: [%v] ", strings.Join(flags, "|")))
// return buffer.String()
// }
//
// // Writes a packet using the writer.  This method tries
// // to limit the side effects of
// func WritePacket(w io.Writer, m *packet) error {
// var enc utils.Encoder
//
// buf := new(bytes.Buffer)
// enc.Uint64(buf, m.format)
// enc.UUID(buf, m.addr.Src().MemberId())
// enc.UUID(buf, m.addr.Dst().MemberId())
// enc.Uint64(buf, m.addr.Src().ChannelId())
// enc.Uint64(buf, m.addr.Dst().ChannelId())
// enc.Uint64(buf, uint64(m.flags))
// enc.Uint64(buf, m.offset)
// enc.Uint64(buf, m.verify)
// enc.Raw(buf, packetReserved)
// enc.Raw(buf, m.data)
//
// // encoding error.
// if err := enc.Err(); err != nil {
// return err
// }
//
// // io error (enc can't be dead!)
// data := buf.Bytes()
// enc.Uint8(w, packetMagic)
// enc.Raw(w, data)
// return enc.Err()
// }
//
// // Reads a packet from an io stream.  Any issues with the
// // stream encoding will result in (nil, err) being returned.
// //
// func ReadPacket(r io.Reader) (*packet, error) {
// var dec utils.Decoder
//
// mag := dec.Uint8(r)
// if mag != packetMagic {
// return nil, PacketDecodingError{"Missing expected delimiter"}
// }
//
// raw := dec.Raw(r)
// if err := dec.Err(); err != nil {
// return nil, ReaderError{err}
// }
//
// buf := bytes.NewBuffer(raw)
//
// format := dec.Uint64(buf)
// srcMemberId := dec.UUID(buf)
// srcChannelId := dec.Uint64(buf)
// dstMemberId := dec.UUID(buf)
// dstChannelId := dec.Uint64(buf)
// flags := dec.Uint64(buf)
// offset := dec.Uint64(buf)
// verify := dec.Uint64(buf)
//
// // reserved space. just ignore (for future header values)
// dec.Raw(buf)
//
// payload := dec.Raw(buf)
//
// // encoding error.
// if err := dec.Err(); err != nil {
// return nil, PacketDecodingError{err.Error()}
// }
//
// src := NewEndPoint(srcMemberId, srcChannelId)
// dst := NewEndPoint(dstMemberId, dstChannelId)
//
// return &packet{
// format: format,
// addr:   NewRemoteAddress(src, dst),
// flags:  utils.BitMask(flags),
// offset: offset,
// verify: verify,
// data:   payload}, nil
// }

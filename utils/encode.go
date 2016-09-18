package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	uuid "github.com/satori/go.uuid"
)

const (
	MaxUint32 = 0xffffffff
)

type OverflowError struct {
	target interface{}
	actual interface{}
}

func (e OverflowError) Error() string {
	return fmt.Sprintf("That value [%v] overflows [%v]", e.actual, e.target)
}

// A simple encoding interface that normalizes the most common, low-level
// is that once an encoding error occurs, this interface guarantees no
// future side effects.
type MessageEncoder interface {
	PutUUID(uuid.UUID)
	PutUint64(uint64)
	PutUint32(uint32)
	PutUint16(uint16)
	PutUint8(uint8)
	PutString(string)
	PutBytes([]byte)

	Err() error
}

type MessageDecoder interface {
	ReadUUID() uuid.UUID
	ReadUint64() uint64
	ReadUint32() uint32
	ReadUint16() uint16
	ReadUint8() uint8
	ReadString() string
	ReadBytes() []byte

	Err() error
}

func NewMessageDecoder(buf *bytes.Buffer) MessageDecoder {
	return &StandardMessageDecoder{buf, nil}
}

func NewMessageEncoder(buf *bytes.Buffer) MessageEncoder {
	return &StandardMessageEncoder{buf, nil}
}

func PutUUID(buf *bytes.Buffer, val uuid.UUID) (err error) {
	_, err = buf.Write(val.Bytes())
	return
}

func ReadUUID(buffer *bytes.Buffer) (u uuid.UUID, err error) {
	buf := make([]byte, 16)
	if _, err = io.ReadFull(buffer, buf); err != nil {
		return
	}

	return uuid.FromBytes(buf)
}

func PutUint64(buffer *bytes.Buffer, val uint64) (err error) {
	buf := make([]byte, binary.MaxVarintLen64)
	num := binary.PutUvarint(buf, val)

	_, err = buffer.Write(buf[:num])
	return
}

func ReadUint64(buf *bytes.Buffer) (uint64, error) {
	return binary.ReadUvarint(buf)
}

func PutUint32(buffer *bytes.Buffer, val uint32) (err error) {
	buf := make([]byte, binary.MaxVarintLen32)
	num := binary.PutUvarint(buf, uint64(val))

	_, err = buffer.Write(buf[:num])
	return
}

func ReadUint32(buf *bytes.Buffer) (uint32, error) {
	val, err := binary.ReadUvarint(buf)
	if err != nil {
		return 0, err
	}

	if val > MaxUint32 {
		return 0, OverflowError{"uint32", val}
	}

	return uint32(val), nil
}

func PutUint16(buffer *bytes.Buffer, val uint16) (err error) {
	return binary.Write(buffer, binary.BigEndian, val)
}

func ReadUint16(buffer *bytes.Buffer) (uint16, error) {
	var ret uint16
	if err := binary.Read(buffer, binary.BigEndian, &ret); err != nil {
		return 0, err
	}

	return ret, nil
}

func PutUint8(buffer *bytes.Buffer, val uint8) (error) {
	return binary.Write(buffer, binary.BigEndian, val)
}

func ReadUint8(buffer *bytes.Buffer) (uint8, error) {
	var ret uint8
	if err := binary.Read(buffer, binary.BigEndian, &ret); err != nil {
		return 0, err
	}

	return ret, nil
}

func PutBytes(buffer *bytes.Buffer, val []byte) (error) {
	if err := PutUint64(buffer, uint64(len(val))); err != nil {
		return err
	}

	_, err := buffer.Write(val)
	return err
}

func ReadBytes(buffer *bytes.Buffer) ([]byte, error) {
	length, err := ReadUint64(buffer)
	if err != nil {
		return nil, err
	}

	tmp := make([]byte, length)
	if _, err = io.ReadFull(buffer, tmp); err != nil {
		return nil, err
	}

	return tmp, nil
}

func PutString(buffer *bytes.Buffer, val string) (error) {
	return PutBytes(buffer, []byte(val))
}

func ReadString(buffer *bytes.Buffer) (string, error) {
	var ret string

	raw, err := ReadBytes(buffer)
	if err != nil {
		return ret, err
	}

	return string(raw), nil
}

type StandardMessageDecoder struct {
	buffer *bytes.Buffer
	err    error
}


func (s *StandardMessageDecoder) ReadUUID() (ret uuid.UUID) {
	if s.err != nil {
		return
	}

	ret, s.err = ReadUUID(s.buffer)
	return
}

func (s *StandardMessageDecoder) ReadUint64() (ret uint64) {
	if s.err != nil {
		return
	}

	ret, s.err = ReadUint64(s.buffer)
	return
}

func (s *StandardMessageDecoder) ReadUint32() (ret uint32) {
	if s.err != nil {
		return
	}

	ret, s.err = ReadUint32(s.buffer)
	return
}

func (s *StandardMessageDecoder) ReadUint16() (ret uint16) {
	if s.err != nil {
		return
	}

	ret, s.err = ReadUint16(s.buffer)
	return
}

func (s *StandardMessageDecoder) ReadUint8() (ret uint8) {
	if s.err != nil {
		return
	}

	ret, s.err = ReadUint8(s.buffer)
	return
}

func (s *StandardMessageDecoder) ReadString() (ret string) {
	if s.err != nil {
		return
	}

	ret, s.err = ReadString(s.buffer)
	return
}

func (s *StandardMessageDecoder) ReadBytes() (ret []byte) {
	if s.err != nil {
		return
	}

	ret, s.err = ReadBytes(s.buffer)
	return
}

func (s *StandardMessageDecoder) Err() error {
	return s.err
}

type StandardMessageEncoder struct {
	buffer *bytes.Buffer
	err    error
}

func (s *StandardMessageEncoder) PutUUID(val uuid.UUID) {
	if s.err != nil {
		return
	}
	s.err = PutUUID(s.buffer, val)
}

func (s *StandardMessageEncoder) PutUint64(val uint64) {
	if s.err != nil {
		return
	}
	s.err = PutUint64(s.buffer, val)
}

func (s *StandardMessageEncoder) PutUint32(val uint32) {
	if s.err != nil {
		return
	}
	s.err = PutUint32(s.buffer, val)
}

func (s *StandardMessageEncoder) PutUint16(val uint16) {
	if s.err != nil {
		return
	}
	s.err = PutUint16(s.buffer, val)
}

func (s *StandardMessageEncoder) PutUint8(val uint8) {
	if s.err != nil {
		return
	}

	s.err = PutUint8(s.buffer, val)
}

func (s *StandardMessageEncoder) PutString(val string) {
	if s.err != nil {
		return
	}
	s.err = PutString(s.buffer, val)
}

func (s *StandardMessageEncoder) PutBytes(val []byte) {
	if s.err != nil {
		return
	}
	s.err = PutBytes(s.buffer, val)
}

func (s *StandardMessageEncoder) Err() error {
	return s.err
}

type noopEncoder int

func (n *noopEncoder) PutUUID(uuid.UUID) {}
func (n *noopEncoder) PutUint64(uint64)  {}
func (n *noopEncoder) PutUint32(uint32)  {}
func (n *noopEncoder) PutUint16(uint16)  {}
func (n *noopEncoder) PutUint8(uint8)    {}
func (n *noopEncoder) PutString(string)  {}
func (n *noopEncoder) PutBytes([]byte)   {}
func (n *noopEncoder) Err() error        { return nil }

type noopDecoder int

func (d *noopDecoder) ReadUUID() (ret uuid.UUID) { return }
func (d *noopDecoder) ReadUint64() (ret uint64)  { return }
func (d *noopDecoder) ReadUint32() (ret uint32)  { return }
func (d *noopDecoder) ReadUint16() (ret uint16)  { return }
func (d *noopDecoder) ReadUint8() (ret uint8)    { return }
func (d *noopDecoder) ReadString() (ret string)  { return }
func (d *noopDecoder) ReadBytes() []byte         { return nil }
func (d *noopDecoder) Err() error                { return nil }

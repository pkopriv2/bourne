package utils

import (
	"encoding/binary"
	"fmt"
	"io"
	"bufio"

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
type MessageWriter interface {
	PutUUID(uuid.UUID)
	PutUint64(uint64)
	PutUint32(uint32)
	PutUint16(uint16)
	PutUint8(uint8)
	PutString(string)
	PutBytes([]byte)

	Flush()

	Err() error
}

type MessageReader interface {
	ReadUUID() uuid.UUID
	ReadUint64() uint64
	ReadUint32() uint32
	ReadUint16() uint16
	ReadUint8() uint8
	ReadString() string
	ReadBytes() []byte

	Err() error
}

func NewMessageReader(reader *bufio.Reader) MessageReader {
	return &StandardMessageReader{reader: reader}
}

func NewMessageWriter(writer *bufio.Writer) MessageWriter {
	return &StandardMessageWriter{writer: writer}
}

func PutUUID(w *bufio.Writer, val uuid.UUID) (err error) {
	_, err = w.Write(val.Bytes())
	return
}

func ReadUUID(r *bufio.Reader) (u uuid.UUID, err error) {
	buf := make([]byte, 16)
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}

	return uuid.FromBytes(buf)
}

func PutUint64(w *bufio.Writer, val uint64) (err error) {
	buf := make([]byte, binary.MaxVarintLen64)
	num := binary.PutUvarint(buf, val)

	_, err = w.Write(buf[:num])
	return
}

func ReadUint64(r *bufio.Reader) (uint64, error) {
	return binary.ReadUvarint(r)
}

func PutUint32(w *bufio.Writer, val uint32) (err error) {
	buf := make([]byte, binary.MaxVarintLen32)
	num := binary.PutUvarint(buf, uint64(val))

	_, err = w.Write(buf[:num])
	return
}

func ReadUint32(r *bufio.Reader) (uint32, error) {
	val, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, err
	}

	if val > MaxUint32 {
		return 0, OverflowError{"uint32", val}
	}

	return uint32(val), nil
}

func PutUint16(w *bufio.Writer, val uint16) (err error) {
	return binary.Write(w, binary.BigEndian, val)
}

func ReadUint16(r *bufio.Reader) (uint16, error) {
	var ret uint16
	if err := binary.Read(r, binary.BigEndian, &ret); err != nil {
		return 0, err
	}

	return ret, nil
}

func PutUint8(w *bufio.Writer, val uint8) (error) {
	return binary.Write(w, binary.BigEndian, val)
}

func ReadUint8(r *bufio.Reader) (uint8, error) {
	var ret uint8
	if err := binary.Read(r, binary.BigEndian, &ret); err != nil {
		return 0, err
	}

	return ret, nil
}

func PutBytes(w *bufio.Writer, val []byte) (error) {
	if err := PutUint64(w, uint64(len(val))); err != nil {
		return err
	}

	_, err := w.Write(val)
	return err
}

func ReadBytes(r *bufio.Reader) ([]byte, error) {
	length, err := ReadUint64(r)
	if err != nil {
		return nil, err
	}

	tmp := make([]byte, length)
	if _, err = io.ReadFull(r, tmp); err != nil {
		return nil, err
	}

	return tmp, nil
}

func PutString(w *bufio.Writer, val string) (error) {
	return PutBytes(w, []byte(val))
}

func ReadString(r *bufio.Reader) (string, error) {
	var ret string

	raw, err := ReadBytes(r)
	if err != nil {
		return ret, err
	}

	return string(raw), nil
}

type StandardMessageReader struct {
	reader *bufio.Reader
	err    error
}


func (s *StandardMessageReader) ReadUUID() (ret uuid.UUID) {
	if s.err != nil {
		return
	}

	ret, s.err = ReadUUID(s.reader)
	return
}

func (s *StandardMessageReader) ReadUint64() (ret uint64) {
	if s.err != nil {
		return
	}

	ret, s.err = ReadUint64(s.reader)
	return
}

func (s *StandardMessageReader) ReadUint32() (ret uint32) {
	if s.err != nil {
		return
	}

	ret, s.err = ReadUint32(s.reader)
	return
}

func (s *StandardMessageReader) ReadUint16() (ret uint16) {
	if s.err != nil {
		return
	}

	ret, s.err = ReadUint16(s.reader)
	return
}

func (s *StandardMessageReader) ReadUint8() (ret uint8) {
	if s.err != nil {
		return
	}

	ret, s.err = ReadUint8(s.reader)
	return
}

func (s *StandardMessageReader) ReadString() (ret string) {
	if s.err != nil {
		return
	}

	ret, s.err = ReadString(s.reader)
	return
}

func (s *StandardMessageReader) ReadBytes() (ret []byte) {
	if s.err != nil {
		return
	}

	ret, s.err = ReadBytes(s.reader)
	return
}

func (s *StandardMessageReader) Err() error {
	return s.err
}

type StandardMessageWriter struct {
	writer *bufio.Writer
	err    error
}

func (s *StandardMessageWriter) Flush() {
	if s.err != nil {
		return
	}
	s.err = s.writer.Flush()
}

func (s *StandardMessageWriter) PutUUID(val uuid.UUID) {
	if s.err != nil {
		return
	}
	s.err = PutUUID(s.writer, val)
}

func (s *StandardMessageWriter) PutUint64(val uint64) {
	if s.err != nil {
		return
	}
	s.err = PutUint64(s.writer, val)
}

func (s *StandardMessageWriter) PutUint32(val uint32) {
	if s.err != nil {
		return
	}
	s.err = PutUint32(s.writer, val)
}

func (s *StandardMessageWriter) PutUint16(val uint16) {
	if s.err != nil {
		return
	}
	s.err = PutUint16(s.writer, val)
}

func (s *StandardMessageWriter) PutUint8(val uint8) {
	if s.err != nil {
		return
	}

	s.err = PutUint8(s.writer, val)
}

func (s *StandardMessageWriter) PutString(val string) {
	if s.err != nil {
		return
	}
	s.err = PutString(s.writer, val)
}

func (s *StandardMessageWriter) PutBytes(val []byte) {
	if s.err != nil {
		return
	}
	s.err = PutBytes(s.writer, val)
}

func (s *StandardMessageWriter) Err() error {
	return s.err
}

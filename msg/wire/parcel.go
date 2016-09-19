package wire

import (
	"bufio"
	"bytes"
	"fmt"
	"log"

	"github.com/pkopriv2/bourne/utils"
	uuid "github.com/satori/go.uuid"
)

// "io"
// "strings"

// Implements an efficient, self describing, messaging packaging algorithm.  This is
// especially useful in the context of a reliable streaming algorithm, which needs
// to efficiently and simultaneously package several independent messages all with
// minimal encoding/decoding overhead.

const (
	ParcelDelimiter = 0x2a
)


type DataType byte

const (
	Uint8  DataType = 0
	Uint16 DataType = 1
	Uint32 DataType = 2
	Uint64 DataType = 3
	String DataType = 4
	UUID   DataType = 5
	Bytes  DataType = 6
)

type MessageId struct {
	val utils.BitMask
}

func NewMessageId(id uint64) MessageId {
	return MessageId{utils.SingleValueMask(id)}
}

// a parcel is simply a collection of messages
type Parcel struct {
	data map[MessageId]interface{}
}

func NewParcel() *Parcel {
	return &Parcel{make(map[MessageId]interface{})}
}

func (s *Parcel) Set(id MessageId, val interface{}) {
	s.data[id] = val
}

func (s *Parcel) Get(m MessageId) (interface{}, bool) {
	val, ok := s.data[m]
	return val, ok
}


func (s *Parcel) Write(writer *bufio.Writer) error {
	return WriteParcel(writer, s)
}

func WriteParcel(writer *bufio.Writer, p *Parcel) error {
	enc := utils.NewMessageWriter(writer)

	// create the messages index
	idx := utils.EmptyBitMask
	for k, _ := range p.data {
		idx |= utils.BitMask(k.val)
	}

	// put the index, followed by the messages
	enc.PutUint64(uint64(idx))
	for _, msg := range utils.SplitMask(idx) {
		val := p.data[NewMessageId(uint64(msg))]

		switch t := val.(type) {
		default:
			panic(fmt.Sprintf("Unsupported type %v", t))
		case uint8:
			enc.PutUint8(uint8(Uint8))
			enc.PutUint8(t)
		case uint16:
			enc.PutUint8(uint8(Uint16))
			enc.PutUint16(t)
		case uint32:
			enc.PutUint8(uint8(Uint32))
			enc.PutUint32(t)
		case uint64:
			enc.PutUint8(uint8(Uint64))
			enc.PutUint64(t)
		case string:
			enc.PutUint8(uint8(String))
			enc.PutString(t)
		case []byte:
			enc.PutUint8(uint8(Bytes))
			enc.PutBytes(t)
		case uuid.UUID:
			enc.PutUint8(uint8(UUID))
			enc.PutUUID(t)
		}
	}

	// finally, put the delimiter
	enc.PutUint8(ParcelDelimiter)
	enc.Flush()

	return enc.Err()
}

func ReadParcel(r *bufio.Reader) (*Parcel, error) {
	raw, err:= r.ReadBytes(ParcelDelimiter)
	if err != nil {
		return nil, err
	}

	dec := utils.NewMessageReader(bufio.NewReader(bytes.NewBuffer(raw)))
	data := make(map[MessageId]interface{})

	idx := utils.BitMask(dec.ReadUint64())
	for _, i := range utils.SplitMask(idx) {
		var id = NewMessageId(uint64(i))

		t := DataType(dec.ReadUint8())
		switch t {
		default:
			log.Printf("Unsupported type %v\n", t)
		case Uint8:
			data[id] = dec.ReadUint8()
		case Uint16:
			data[id] = dec.ReadUint16()
		case Uint32:
			data[id] = dec.ReadUint32()
		case Uint64:
			data[id] = dec.ReadUint64()
		case String:
			data[id] = dec.ReadString()
		case UUID:
			data[id] = dec.ReadUUID()
		case Bytes:
			data[id] = dec.ReadBytes()
		}
	}

	if err := dec.Err(); err != nil {
		return nil, err
	}

	return &Parcel{data}, nil
}

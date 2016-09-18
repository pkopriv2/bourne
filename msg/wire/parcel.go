package wire

import (
	"bytes"
	"fmt"

	"github.com/pkopriv2/bourne/utils"
	uuid "github.com/satori/go.uuid"
)

// "io"
// "strings"

// Implements an efficient, self describing, messaging packaging algorithm.  This is
// especially useful in the context of a reliable streaming algorithm, which needs
// to efficiently and simultaneously package several independent messages all with
// minimal enc.Putding/decoding overhead.

const (
	ParcelLowBound = 1<<8
	ParcelHighBound = 1<<10
)

// the "natively" supported types
type DataType byte

const (
	Uint8  DataType = iota
	Uint16
	Uint32
	Uint64
	String
	UUID
	Bytes
)

type MessageId utils.BitMask
type Parcel struct {
	data map[MessageId]interface{}
}

func NewParcel() *Parcel {
	return &Parcel{make(map[MessageId]interface{})}
}

func (s *Parcel) Set(m MessageId, val interface{}) {
	s.data[m] = val
}

func (s *Parcel) Get(m MessageId) interface{} {
	return s.data[m]
}

func Pack(b Parcel) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, ParcelLowBound, ParcelHighBound))
	enc := utils.NewMessageEncoder(buf)

	// create the messages index
	idx := utils.EmptyBitMask
	for k, _ := range b.data {
		idx |= utils.BitMask(k)
	}

	// put the headers
	enc.PutUint64(uint64(idx))

	// put each message
	for _, msg := range utils.SplitMask(idx) {
		val := b.data[MessageId(msg)]

		switch t := val.(type) {
		default:
			panic(fmt.Sprintf("Unsupported type %v", val))
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

	if err := enc.Err(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func Unpack(val []byte) (*Parcel, error)  {
	buf := bytes.NewBuffer(val)
	dec := utils.NewMessageDecoder(buf)

	data := make(map[MessageId]interface{})

	idx := utils.BitMask(dec.ReadUint64())
	for _, i := range utils.SplitMask(idx) {
		var id = MessageId(i)

		switch DataType(dec.ReadUint8()) {
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
		}
	}

	if err := dec.Err(); err != nil {
		return nil, err
	}
	return &Parcel{data}, nil
}


func isSupported(val interface{}) bool {
	switch val.(type) {
	default:
		return false
	case uint8, uint16, uint32, uint64, string, []byte, uuid.UUID:
		return true
	}
}


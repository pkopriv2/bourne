package amoeba

import (
	"bytes"
	"math/big"
	"strings"
	"time"

	uuid "github.com/satori/go.uuid"
)

var (
	ZeroInt    int
	ZeroUUID   uuid.UUID
	ZeroString string
	ZeroTime   time.Time
)

// Just a list of supported data types.

// An integer key type
type IntKey int

func (i IntKey) Inc() IntKey {
	return IntKey(i + 1)
}

func (i IntKey) Compare(s Key) int {
	return int(i - s.(IntKey))
}

type IntDescKey int

func (i IntDescKey) Inc() IntDescKey {
	return IntDescKey(i + 1)
}

func (i IntDescKey) Compare(s Key) int {
	return int(s.(IntDescKey)-i)
}

// A string key type
type StringKey string

func (i StringKey) Inc() StringKey {
	return StringKey(IncrementString(string(i)))
}

func (i StringKey) Compare(s Key) int {
	return strings.Compare(string(i), string(s.(StringKey)))
}

// A byte array key type.
type BytesKey []byte

// Increments the byte array by one value.  This assumes big.Endian
// encoding.
func (i BytesKey) Inc() BytesKey {
	return BytesKey(IncrementBytes(i))
}

func (i BytesKey) Compare(s Key) int {
	return bytes.Compare([]byte(i), []byte(s.(BytesKey)))
}

// A byte array key type.
type UUIDKey uuid.UUID

// Increments the byte array by one value.  This assumes big.Endian
// encoding.
func (k UUIDKey) Inc() UUIDKey {
	return UUIDKey(IncrementUUID(uuid.UUID(k)))
}

func (k UUIDKey) Compare(s Key) int {
	return bytes.Compare(uuid.UUID(k).Bytes(), uuid.UUID(s.(UUIDKey)).Bytes())
}

// a few helper methods
func CompareBytes(a, b []byte) int {
	return bytes.Compare(a, b)
}

func CompareUUIDs(a, b uuid.UUID) int {
	return CompareBytes(a.Bytes(), b.Bytes())
}

func CompareStrings(a, b string) int {
	return CompareBytes([]byte(a), []byte(b))
}

func IncrementBytes(val []byte) []byte {
	cur := new(big.Int)
	cop := make([]byte, len(val))
	copy(cop, val)

	cur.SetBytes(val)
	cur.Add(cur, big.NewInt(1))
	return cur.Bytes()
}

// TODO: THERE'S A BUG!!
func IncrementUUID(id uuid.UUID) uuid.UUID {
	inc := IncrementBytes(id.Bytes())
	buf := make([]byte, 16)
	for i, b := range inc {
		buf[i] = b
	}

	id, err := uuid.FromBytes(buf)
	if err != nil {
		panic(err)
	}

	return id
}

func IncrementString(val string) string {
	return string(IncrementBytes([]byte(val)))
}

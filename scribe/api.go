package scribe

import (
	"fmt"
	"reflect"

	uuid "github.com/satori/go.uuid"
)

// This package implements a very simple message formating/encoding scheme
// over the standard go schemes (e.g. json,gob,protobuf,etc...).  The
// motivation for this library was the lack of abstraction around the
// the current encoding schemes.  Most require that the complete request
// and response structures are known at compile time.  In highly abstract
// environments, this necessarily requires leaking of underlying details.
// Many encoding schemes only access exported fields, which means fields
// are made public that otherwise wouldn't.  An argument could be made
// that by being serialized onto a wire, it forces the leaking of the
// the details, but assuming a "parser" function is known at compile
// none of the consuming code is intrinsically forced to understand the
// specific details of the data being sent.  This is really only a requirement
// when external system compatibility is required.
//
// This provides a very simple, "map-like" format for consumers to define
// implementation specific details.  This allows for both backwards
// and forwards compatibility (at an api and binary level), while still
// being compatible with the go internal formats.  Messages are fully
// encodable/decodable on a standard encoding scheme.  So far, this seems
// like a good balance between verbosity and future proofing.
//
//
// To enable conversion to Messages, objects must implement the writer interface.
// ```
//  type MyObject struct {
//    field int
//  }
//
//  func (m *MyObject) Write(w Writer) {
//     w.Write("field", m.int)
//  }
//
// ```

// To be returned when a requested field does not exist.
type MissingFieldError struct {
	field string
}

func (m *MissingFieldError) Error() string {
	return fmt.Sprintf("Missing field [%v].", m.field)
}

// To be returned when a requested field does not exist.
type IncompatibleTypeError struct {
	expected string
	actual   string
}

func NewIncompatibleTypeError(e interface{}, a interface{}) *IncompatibleTypeError {
	return &IncompatibleTypeError{reflect.TypeOf(e).String(), reflect.TypeOf(a).String()}
}

func (m *IncompatibleTypeError) Error() string {
	return fmt.Sprintf("Incompatible types. Expected [%v]; Actual [%v]", m.expected, m.actual)
}

// Returned when an unknown type is encountered.
type UnsupportedTypeError struct {
	actual string
}

func NewUnsupportedTypeError(e interface{}) *UnsupportedTypeError {
	return &UnsupportedTypeError{reflect.TypeOf(e).String()}
}

func (u *UnsupportedTypeError) Error() string {
	return fmt.Sprintf("Unsupported type: %v", u.actual)
}

// A primary consumer abstraction.  Consumers wishing to define a simple
// encoding scheme for their objects should implement this interface. Any
// writable object is automatically embeddable in other objects and
// standard encoding streams (e.g. json/gob/xml).
type Writable interface {
	Write(Writer)
}

// The primary encoding interface. Consumers use the writer to populate
// the fields of a message
type Writer interface {
	WriteBool(field string, val bool)
	WriteBools(field string, val []bool)
	WriteString(field string, val string)
	WriteStrings(field string, val []string)
	WriteMessage(field string, val Writable)
	WriteMessages(field string, val interface{}) // must be a pointer to an array of writables.

	// Supported Extensions
	WriteInt(field string, val int)
	WriteInts(field string, val []int)
	WriteBytes(field string, val []byte)
	WriteUUID(field string, val uuid.UUID)
}

// The primary decoding interface. Consumers use the reader to populate
// the fields of an object.
type Reader interface {
	ReadBool(field string, val *bool) error
	ReadBools(field string, val *[]bool) error
	ReadString(field string, val *string) error
	ReadStrings(field string, val *[]string) error
	ReadMessage(field string, val *Message) error
	ReadMessages(field string, val *[]Message) error

	// Supported extensions
	ReadInt(field string, val *int) error
	ReadInts(field string, val *[]int) error
	ReadBytes(field string, val *[]byte) error
	ReadUUID(field string, val *uuid.UUID) error
}

// An immutable data object.  A message may be embedded in other messages
// or serialized onto data streams as desired.  They may also be passed
// to parsing functions for populating internal data fields.  This does
// force consumers to make public parsers where an implementation is required
type Message interface {
	Reader
	Streamer
	Writable
}

// A universal wrapper over the gob/json encoders.  Messages can be encoded
// onto streams of these formats for free.
type Encoder interface {
	Encode(interface{}) error
}

// A universal wrapper over the gob/json decoders.  Messages can be decoded
// onto streams of these formats for free.
type Decoder interface {
	Decode(interface{}) error
}

type Streamer interface {
	Stream(Encoder) error
}

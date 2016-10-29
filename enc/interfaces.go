package enc

import (
	"fmt"
	"reflect"
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
// Messages support the full complement of data types for values, and are fully
// nestable.
//
//   * int
//   * int8
//   * int16
//   * int32
//   * int64
//   * uint
//   * ...
//   * string
//   * bool
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
type UnsupportedTypeError struct {
	actual string
}

func NewUnsupportedTypeError(e reflect.Type) *UnsupportedTypeError {
	return &UnsupportedTypeError{e.String()}
}

func (m *UnsupportedTypeError) Error() string {
	return fmt.Sprintf("Unsupported type [%v]", m.actual)
}

// To be returned when a requested field does not exist.
type IncompatibleTypeError struct {
	expected string
	actual   string
}

func NewIncompatibleTypeError(e reflect.Type, a reflect.Type) *IncompatibleTypeError {
	return &IncompatibleTypeError{e.String(), a.String()}
}

func (m *IncompatibleTypeError) Error() string {
	return fmt.Sprintf("Incompatible types. Expected [%v]; Actual [%v]", m.expected, m.actual)
}

// The primary encoding interface. Consumers use the writer to populate
// the fields of a message
type Writer interface {
	Write(field string, value interface{})
}

// The primary decoding interface. Consumers use the reader to populate
// the fields of an object.
type Reader interface {
	Read(field string, value interface{}) error
	ReadOptional(field string, value interface{}) (bool, error)
}

// A primary consumer abstraction.  Consumers wishing to define a simple
// encoding scheme for their objects should implement this interface. Any
// writable object is automatically embeddable in other objects and
// standard encoding streams (e.g. json/gob/xml).
type Writable interface {
	Write(Writer)
}

// An immutable data object.  A message may be embedded in other messages
// or serialized onto data streams as desired.  They may also be passed
// to parsing functions for populating internal data fields.  This does
// force consumers to make public parsers where an implementation is required
type Message interface {
	Reader
	Streamable
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

type Streamable interface {
	Stream(Encoder) error
}
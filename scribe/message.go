package scribe

import (
	"reflect"
	"strconv"

	"github.com/pkg/errors"
)

// Builds a message from the given builder func
var EmptyMessage = newMessageWriter().Build()

// Builds a message from the given builder func
func Build(fn func(w Writer)) Message {
	// initialize a new builder
	msg := newMessageWriter()

	// invoke the builder function
	fn(msg)

	// return the results
	return msg.Build()
}

// Encodes the writable onto a message and returns it.
func Write(w Writable) Message {
	return Build(w.Write)
}

// Encodes a writable onto the stream.
func Encode(enc Encoder, w Writable) error {
	return Write(w).Stream(enc)
}

// Decodes a message from the stream.
func Decode(e Decoder) (Message, error) {
	var raw map[string]interface{}
	if err := e.Decode(&raw); err != nil {
		return nil, errors.Wrapf(err, "Error reading message from stream")
	}

	obj, err := parseObject(raw)
	if err != nil {
		return nil, errors.Wrapf(err, "Error parsing object from stream")
	}

	return &message{obj}, nil
}

type messageWriter struct {
	val Object
}

func newMessageWriter() *messageWriter {
	return &messageWriter{newEmptyObject()}
}

func (m *messageWriter) Write(field string, raw interface{}) {
	if isNil(raw) {
		return
	}

	// Derivative types
	switch typed := raw.(type) {
	case int:
		m.Write(field, writeInt(typed))
		return
	case []int:
		m.Write(field, writeInts(typed))
		return
	}

	// Primitive types
	switch typed := raw.(type) {
	case string:
		m.val[field] = String(typed)
		return
	case bool:
		m.val[field] = Bool(typed)
		return
	case Value:
		m.val[field] = typed
		return
	case Writable:
		m.val[field] = Write(typed).(*message).val
		return
	}

	if !isArrayValue(raw) {
		panic(errors.Wrapf(NewUnsupportedTypeError(raw), "Error writing field [%v]", field))
	}

	// Array types
	arr := reflect.ValueOf(raw)
	num := arr.Len()
	val := make([]Value, num)
	for i := 0; i < num; i++ {
		item := arr.Index(i).Interface()
		switch typed := item.(type) {
		case string:
			val[i] = String(typed)
		case bool:
			val[i] = Bool(typed)
		case Value:
			val[i] = typed
		case Writable:
			val[i] = Write(typed).(*message).val
		}
	}

	m.val[field] = Array(val)
}

func (m *messageWriter) Build() Message {
	return &message{m.val.Copy()}
}

type message struct {
	val Object
}

func (m *message) Read(field string, raw interface{}) error {
	value, ok := m.val[field]
	if !ok {
		return errors.Wrap(&MissingFieldError{field}, "Error reading field")
	}

	// Derivative types
	switch ptr := raw.(type) {
	case *int:
		var str string
		if err := m.Read(field, &str); err != nil {
			return err
		}

		return assignInt(str, ptr)
	case *[]int:
		var str []string
		if err := m.Read(field, &str); err != nil {
			return err
		}

		return assignInts(str, ptr)
	case *Reader:
		var msg Message
		if err := m.Read(field, &msg); err != nil {
			return err
		}

		*ptr = msg
		return nil
	case *[]Reader:
		var msgs []Message
		if err := m.Read(field, &msgs); err != nil {
			return errors.Wrapf(err, "Error while reading field [%v]", field)
		}

		ret := make([]Reader, len(msgs))
		for i := 0; i < len(msgs); i++ {
			ret[i] = msgs[i]
		}

		*ptr = ret
		return nil
	}

	// Primitive types
	switch ptr := raw.(type) {
	default:
		if err := value.AssignTo(ptr); err != nil {
			return errors.Wrapf(err, "Error while reading field [%v]", field)
		}
	case *Message:
		var obj Object
		if err := value.AssignTo(&obj); err != nil {
			return errors.Wrapf(err, "Error while reading field [%v]", field)
		}

		*ptr = &message{obj}
	case *[]Message:
		var obj []Object
		if err := value.AssignTo(&obj); err != nil {
			return errors.Wrapf(err, "Error while reading field [%v]", field)
		}

		val := make([]Message, len(obj))
		for i := 0; i < len(obj); i++ {
			val[i] = &message{obj[i]}
		}

		*ptr = val
	}

	return nil
}

func (m *message) ReadOptional(field string, ptr interface{}) (bool, error) {
	err := m.Read(field, ptr)
	if err == nil {
		return true, nil
	}


	if _, ok := errors.Cause(err).(*MissingFieldError); ok {
		return false, nil
	}

	return false, err
}

func (m *message) Stream(e Encoder) error {
	return e.Encode(m.val.Dump())
}

func (m *message) Write(w Writer) {
	m.val.Write(w)
}

// helper functions

func assignInt(val string, ptr *int) error {
	i, err := strconv.Atoi(val)
	if err != nil {
		return err
	}

	*ptr = i
	return nil
}

func assignInts(vals []string, ptr *[]int) error {
	ret := make([]int, len(vals))
	for i := 0; i < len(vals); i++ {
		if err := assignInt(vals[i], &ret[i]); err != nil {
			return err
		}
	}

	*ptr = ret
	return nil
}

func writeInt(val int) Value {
	return String(strconv.Itoa(val))
}

func writeInts(val []int) Value {
	ret := make([]Value, len(val))
	for i := 0; i < len(val); i++ {
		ret[i] = writeInt(val[i])
	}

	return Array(ret)
}

func isArrayValue(value interface{}) bool {
	switch reflect.ValueOf(value).Kind() {
	default:
		return false
	case reflect.Array:
		return true
	case reflect.Slice:
		return true
	}
}

func isNil(value interface{}) bool {
	if !reflect.ValueOf(value).IsValid() {
		return true
	}

	val := reflect.ValueOf(value)
	if val.Kind() == reflect.Ptr && val.IsNil() {
		return true
	}

	return false
}

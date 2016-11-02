package enc

import (
	"encoding/gob"
	"reflect"
	"strconv"

	"github.com/pkg/errors"
)

// Message builders are just functions which accept a writer.
type WriteFn func(w Writer)

// Encodes a writable onto the stream.
func StreamWrite(enc Encoder, w Writable) error {
	return Write(w).Stream(enc)
}

// Decodes a message from the stream.
func StreamRead(e Decoder) (Message, error) {
	var raw map[string]interface{}
	if err := e.Decode(&raw); err != nil {
		return nil, errors.Wrapf(err, "Error reading message from stream")
	}

	obj, err := ParseDumpedObject(raw)
	if err != nil {
		return nil, errors.Wrapf(err, "Error parsing object from stream")
	}

	return &message{obj}, nil
}

// Builds a message from the given builder func
func Build(fn WriteFn) Message {
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

// Build a small type system over the standard go system.  The intent is to
// severely limit the number of encodable values so that we can maintain
// compatibility across various encoding schemes.
type Value interface {
	AssignTo(ptr interface{}) error
	Dump() interface{}
}

// A boolean value
type Bool bool

func (b Bool) Dump() interface{} {
	return bool(b)
}

func (b Bool) AssignTo(raw interface{}) error {
	switch ptr := raw.(type) {
	default:
		return NewIncompatibleTypeError(b, ptr)
	case *bool:
		*ptr = bool(b)
	}

	return nil
}

// a string value
type String string

func (s String) Dump() interface{} {
	return string(s)
}

func (s String) AssignTo(raw interface{}) error {
	switch ptr := raw.(type) {
	default:
		return NewIncompatibleTypeError(s, ptr)
	case *string:
		*ptr = string(s)
	}

	return nil
}

// an array value (composed of other values)
type Array []Value

func ParseDumpedArray(arr []interface{}) (Array, error) {
	ret := make([]Value, 0, len(arr))
	for _, cur := range arr {
		val, err := ParseDumpedValue(cur)
		if err != nil {
			return nil, err
		}

		ret = append(ret, val)
	}

	return Array(ret), nil
}

func (a Array) AssignTo(ptr interface{}) error {
	switch ptr := ptr.(type) {
	default:
		return NewUnsupportedTypeError(ptr)
	case *[]Object:
		arr := make([]Object, len(a))
		for i := 0; i < len(a); i++ {
			a[i].AssignTo(&arr[i])
		}
		*ptr = arr
	case *[]string:
		arr := make([]string, len(a))
		for i := 0; i < len(a); i++ {
			a[i].AssignTo(&arr[i])
		}
		*ptr = arr
	case *[]bool:
		arr := make([]bool, len(a))
		for i := 0; i < len(a); i++ {
			a[i].AssignTo(&arr[i])
		}
		*ptr = arr
	}

	return nil
}

func (a Array) Dump() interface{} {
	arr := make([]interface{}, len(a))
	gob.Register(arr) // grr....
	for i := 0; i < len(a); i++ {
		arr[i] = a[i].Dump()
	}
	return arr
}

// a generic field indexed value
type Object map[string]Value

func NewEmptyObject() Object {
	return Object(make(map[string]Value))
}

func ParseDumpedObject(data map[string]interface{}) (Object, error) {
	obj := make(map[string]Value)
	for k, v := range data {
		val, err := ParseDumpedValue(v)
		if err != nil {
			return nil, err
		}

		obj[k] = val
	}

	return Object(obj), nil
}

func (o Object) AssignTo(ptr interface{}) error {
	switch ptr := ptr.(type) {
	default:
		return NewUnsupportedTypeError(ptr)
	case *Object:
		*ptr = o
	}

	return nil
}

func (o Object) Read(field string, ptr interface{}) error {
	value, ok := o[field]
	if !ok {
		return &MissingFieldError{field}
	}

	if err := value.AssignTo(ptr); err != nil {
		return errors.Wrapf(err, "Error while reading field [%v]", field)
	}

	return nil
}

func (o Object) ReadOptional(field string, ptr interface{}) (bool, error) {
	value, ok := o[field]
	if !ok {
		return false, nil
	}

	if err := value.AssignTo(ptr); err != nil {
		return false, errors.Wrapf(err, "Error while reading field [%v]", field)
	}

	return true, nil
}

func (o Object) Write(w Writer) {
	for k, v := range o {
		w.Write(k, v)
	}
}

func (o Object) Copy() Object {
	ret := make(map[string]Value)
	for k, v := range o {
		ret[k] = v
	}

	return Object(ret)
}

func (o Object) Dump() interface{} {
	ret := make(map[string]interface{})
	for k, v := range o {
		ret[k] = v.Dump()
	}

	return ret
}

// parses the dumped format of value
func ParseDumpedValue(data interface{}) (Value, error) {
	switch val := data.(type) {
	default:
		return nil, NewUnsupportedTypeError(val)
	case bool:
		return Bool(val), nil
	case string:
		return String(val), nil
	case []interface{}:
		return ParseDumpedArray(val)
	case map[string]interface{}:
		return ParseDumpedObject(val)
	}
}

type messageWriter struct {
	val Object
}

func newMessageWriter() *messageWriter {
	return &messageWriter{NewEmptyObject()}
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
		return &MissingFieldError{field}
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

	if _, ok := err.(*MissingFieldError); ok {
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

package scribe

import (
	"encoding/gob"
	"reflect"
	"strconv"

	"github.com/pkg/errors"
)

// Builds a message from the given builder func
var EmptyMessage = newMessageWriter().Build()

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

// Build a small type system over the standard go system.  The intent is to
// severely limit the number of encodable values so that we can maintain
// compatibility across various encoding schemes.
type encValue interface {
	// Assigns the value to the value pointed to by ptr.   Values are able
	// to make decisions as to what ptr values it supports.
	AssignTo(ptr interface{}) error

	// Dumps the value to a raw go type
	Dump() interface{}
}

// A boolean value
type boolValue bool

func (b boolValue) AssignTo(raw interface{}) error {
	switch ptr := raw.(type) {
	default:
		return NewIncompatibleTypeError(b, ptr)
	case *bool:
		*ptr = bool(b)
	}

	return nil
}

func (b boolValue) Dump() interface{} {
	return bool(b)
}

// a string value
type stringValue string

func (s stringValue) AssignTo(raw interface{}) error {
	switch ptr := raw.(type) {
	default:
		return NewIncompatibleTypeError(s, ptr)
	case *string:
		*ptr = string(s)
	}

	return nil
}

func (s stringValue) Dump() interface{} {
	return string(s)
}

// an array value (composed of other values)
type arrayValue []encValue

func (a arrayValue) AssignTo(ptr interface{}) error {
	switch ptr := ptr.(type) {
	default:
		return NewUnsupportedTypeError(ptr)
	case *[]objectValue:
		arr := make([]objectValue, len(a))
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

func (a arrayValue) Dump() interface{} {
	arr := make([]interface{}, len(a))
	gob.Register(arr) // grr.... what does this screw up later?
	for i := 0; i < len(a); i++ {
		arr[i] = a[i].Dump()
	}
	return arr
}

func parseArray(arr []interface{}) (arrayValue, error) {
	ret := make([]encValue, 0, len(arr))
	for _, cur := range arr {
		val, err := parseValue(cur)
		if err != nil {
			return nil, err
		}

		ret = append(ret, val)
	}

	return arrayValue(ret), nil
}

// a generic field indexed value
type objectValue map[string]encValue

func (o objectValue) AssignTo(ptr interface{}) error {
	switch ptr := ptr.(type) {
	default:
		return NewUnsupportedTypeError(ptr)
	case *objectValue:
		*ptr = o
	}

	return nil
}

func (o objectValue) Read(field string, ptr interface{}) error {
	value, ok := o[field]
	if !ok {
		return &MissingFieldError{field}
	}

	if err := value.AssignTo(ptr); err != nil {
		return errors.Wrapf(err, "Error while reading field [%v]", field)
	}

	return nil
}

func (o objectValue) ReadOptional(field string, ptr interface{}) (bool, error) {
	value, ok := o[field]
	if !ok {
		return false, nil
	}

	if err := value.AssignTo(ptr); err != nil {
		return false, errors.Wrapf(err, "Error while reading field [%v]", field)
	}

	return true, nil
}

func (o objectValue) Write(w Writer) {
	for k, v := range o {
		w.Write(k, v)
	}
}

func (o objectValue) Copy() objectValue {
	ret := make(map[string]encValue)
	for k, v := range o {
		ret[k] = v
	}

	return objectValue(ret)
}

func (o objectValue) Dump() interface{} {
	ret := make(map[string]interface{})
	for k, v := range o {
		ret[k] = v.Dump()
	}

	return ret
}

func newEmptyObject() objectValue {
	return objectValue(make(map[string]encValue))
}

func parseObject(data map[string]interface{}) (objectValue, error) {
	obj := make(map[string]encValue)
	for k, v := range data {
		val, err := parseValue(v)
		if err != nil {
			return nil, err
		}

		obj[k] = val
	}

	return objectValue(obj), nil
}

// parses the dumped format of value
func parseValue(data interface{}) (encValue, error) {
	switch val := data.(type) {
	default:
		return nil, NewUnsupportedTypeError(val)
	case bool:
		return boolValue(val), nil
	case string:
		return stringValue(val), nil
	case []interface{}:
		return parseArray(val)
	case map[string]interface{}:
		return parseObject(val)
	}
}

type messageWriter struct {
	val objectValue
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
		m.val[field] = stringValue(typed)
		return
	case bool:
		m.val[field] = boolValue(typed)
		return
	case encValue:
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
	val := make([]encValue, num)
	for i := 0; i < num; i++ {
		item := arr.Index(i).Interface()
		switch typed := item.(type) {
		case string:
			val[i] = stringValue(typed)
		case bool:
			val[i] = boolValue(typed)
		case encValue:
			val[i] = typed
		case Writable:
			val[i] = Write(typed).(*message).val
		}
	}

	m.val[field] = arrayValue(val)
}

func (m *messageWriter) Build() Message {
	return &message{m.val.Copy()}
}

type message struct {
	val objectValue
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
		var obj objectValue
		if err := value.AssignTo(&obj); err != nil {
			return errors.Wrapf(err, "Error while reading field [%v]", field)
		}

		*ptr = &message{obj}
	case *[]Message:
		var obj []objectValue
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

func writeInt(val int) encValue {
	return stringValue(strconv.Itoa(val))
}

func writeInts(val []int) encValue {
	ret := make([]encValue, len(val))
	for i := 0; i < len(val); i++ {
		ret[i] = writeInt(val[i])
	}

	return arrayValue(ret)
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

package scribe

import (
	"encoding/base64"
	"reflect"
	"strconv"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// Version tagged with each message.
// TODO: Start using this!
var Version = "0.1"

// Builds a message from the given builder func
var EmptyMessage = newWriter().Build()

// Parses a message using the given parser and assigns the return value
// the input pointer.
func ParseMessage(val Message, ptr interface{}, fn Parser) error {
	new, err := fn(val)
	if err != nil {
		return err
	}

	ptrReflect := reflect.ValueOf(ptr)
	newReflect := reflect.ValueOf(new)
	if ptrReflect.Kind() != reflect.Ptr || ptrReflect.IsNil() {
		return NewIncompatibleTypeError(new, val)
	}
	ptrReflect.Elem().Set(newReflect)
	return nil
}

func ParseMessages(msgs []Message, ptr interface{}, fn Parser) error {
	valReflect := reflect.ValueOf(ptr)
	if valReflect.Kind() != reflect.Ptr {
		return NewIncompatibleTypeError(ptr, ptr)
	}

	slice := reflect.MakeSlice(reflect.TypeOf(ptr).Elem(), len(msgs), len(msgs))
	for i, m := range msgs {
		new, err := fn(m)
		if err != nil {
			return err
		}

		slice.Index(i).Set(reflect.ValueOf(new))
	}

	valReflect.Elem().Set(slice)
	return nil
}

// Parses a message from the given bytes.  This assumes
// the message was encoding with: Message#Bytes()
func Parse(val []byte) (Message, error) {
	obj, err := parseObjectFromBytes(val)
	if err != nil {
		return nil, err
	}

	return message(obj), nil
}

// Builds a message from the given builder func
func Build(fn func(w Writer)) (msg Message) {
	writer := newWriter()
	defer func() { msg = writer.Build() }()

	fn(writer)
	return
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
	err := e.Decode(&raw)
	if err != nil {
		return nil, err
	}

	obj, err := parseObject(raw)
	if err != nil {
		return nil, err
	}

	return message(obj), nil
}

type writer Object

func newWriter() writer {
	return writer(newEmptyObject())
}

func (w writer) WriteBool(field string, val bool) {
	w[field] = Bool(val)
}

func (w writer) WriteString(field string, val string) {
	w[field] = String(val)
}

func (w writer) WriteMessage(field string, val Writable) {
	w[field] = Object(Write(val).(message))
}

// internal helper
func (w writer) WriteArray(field string, val Array) {
	w[field] = val
}

// internal helper
func (w writer) WriteObject(field string, val Object) {
	w[field] = val
}

func (w writer) WriteBools(field string, val []bool) {
	w[field] = newBoolArray(val)
}

func (w writer) WriteStrings(field string, val []string) {
	w[field] = newStringArray(val)
}

func (w writer) WriteMessages(field string, raw interface{}) {
	if !isArrayValue(raw) {
		panic(errors.Wrapf(NewUnsupportedTypeError(raw), "Error writing field [%v]", field))
	}

	arr := reflect.ValueOf(raw)
	num := arr.Len()
	val := make([]Object, num)
	for i := 0; i < num; i++ {
		item := arr.Index(i).Interface()

		switch typed := item.(type) {
		default:
			panic(errors.Wrapf(NewUnsupportedTypeError(item), "Error writing field [%v]", field))
		case Writable:
			val[i] = Write(typed).(message).Raw()
		}
	}

	w[field] = newObjectArray(val)
}

func (w writer) WriteInt(field string, val int) {
	w.WriteString(field, strconv.Itoa(val))
}

func (w writer) WriteInts(field string, val []int) {
	strs := make([]string, 0, len(val))
	for _, s := range val {
		strs = append(strs, strconv.Itoa(s))
	}
	w.WriteStrings(field, strs)
}

func (w writer) WriteBytes(field string, val []byte) {
	w.WriteString(field, base64.StdEncoding.EncodeToString(val))
}

func (w writer) WriteUUID(field string, val uuid.UUID) {
	w.WriteString(field, val.String())
}

func (w writer) Raw() Object {
	return Object(w).Copy()
}

func (w writer) Build() Message {
	return message(w.Raw())
}

type message Object

func (m message) ReadBool(field string, val *bool) error {
	return Object(m).Read(field, val)
}

func (m message) ReadBools(field string, val *[]bool) error {
	return Object(m).Read(field, val)
}

func (m message) ReadString(field string, val *string) error {
	return Object(m).Read(field, val)
}

func (m message) ReadStrings(field string, val *[]string) error {
	return Object(m).Read(field, val)
}

func (m message) ReadMessage(field string, val *Message) error {
	var raw Object
	if err := Object(m).Read(field, &raw); err != nil {
		return err
	}

	*val = message(raw)
	return nil
}

func (m message) ParseMessage(field string, ptr interface{}, fn Parser) error {
	var raw Object
	if err := Object(m).Read(field, &raw); err != nil {
		return err
	}

	return ParseMessage(message(raw), ptr, fn)
}

func (m message) ParseMessages(field string, val interface{}, fn Parser) error {
	var msgs []Message
	if err := m.ReadMessages(field, &msgs); err != nil {
		return err
	}

	return ParseMessages(msgs, val, fn)
}

func (m message) ReadOptionalMessage(field string, val *Message) error {
	var raw Object
	if ok, err := Object(m).ReadOptional(field, &raw); !ok || err != nil {
		return err
	}

	*val = message(raw)
	return nil
}

func (m message) ReadMessages(field string, val *[]Message) error {
	var raw []Object
	if err := Object(m).Read(field, &raw); err != nil {
		return err
	}

	ret := make([]Message, 0, len(raw))
	for _, v := range raw {
		ret = append(ret, message(v))
	}

	*val = ret
	return nil
}

func (m message) ReadInt(field string, val *int) error {
	var str string
	if err := m.ReadString(field, &str); err != nil {
		return err
	}

	i, err := strconv.Atoi(str)
	if err != nil {
		return errors.Wrapf(err, "Unable to convert [%v] to int", str)
	}

	*val = i
	return nil
}

func (m message) ReadInts(field string, val *[]int) error {
	var strs []string
	if err := m.ReadStrings(field, &strs); err != nil {
		return err
	}

	ret := make([]int, 0, len(strs))
	for _, s := range strs {
		i, err := strconv.Atoi(s)
		if err != nil {
			return errors.Wrapf(err, "Unable to convert [%v] to int", s)
		}

		ret = append(ret, i)
	}

	*val = ret
	return nil
}

func (m message) ReadBytes(field string, val *[]byte) error {
	var str string
	if err := m.ReadString(field, &str); err != nil {
		return err
	}

	bytes, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return errors.Wrapf(err, "Unable to decode [%v] to bytes", str)
	}

	*val = bytes
	return nil
}

func (m message) ReadUUID(field string, val *uuid.UUID) error {
	var str string
	if err := m.ReadString(field, &str); err != nil {
		return err
	}

	id, err := uuid.FromString(str)
	if err != nil {
		return errors.Wrapf(err, "Unable to convert [%v] to uuid", str)
	}

	*val = id
	return nil
}

func (m message) Stream(e Encoder) error {
	return e.Encode(Object(m).Dump())
}

func (m message) Bytes() []byte {
	return Object(m).Bytes()
}

func (m message) Write(w Writer) {
	for k, v := range m {
		switch t := v.(type) {
		case Bool:
			w.WriteBool(k, bool(t))
		case String:
			w.WriteString(k, string(t))
		case Object:
			w.(writer).WriteObject(k, t)
		case Array:
			w.(writer).WriteArray(k, t)
		}
	}
}

func (m message) Raw() Object {
	return Object(m).Copy()
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

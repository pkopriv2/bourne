package enc

import (
	"errors"
	"fmt"
	"reflect"
)

// To be returned with a nil value is erroneously detected.
var nilValueErrorError = errors.New("Nil value")

// Decodes a message from the stream.
func DecodeMessage(e Decoder) (Message, error) {
	var raw map[string]interface{}
	if err := e.Decode(&raw); err != nil {
		return nil, err
	}

	return newMessage(raw), nil
}

// Encodes the message onto the stream.
func EncodeMessage(enc Encoder, msg Message) error {
	return msg.Stream(enc)
}

// Encodes a writable onto the stream.
func EncodeWritable(enc Encoder, w Writable) error {
	msg, err := BuildMessage(w.Write)
	if err != nil {
		return err
	}

	return EncodeMessage(enc, msg)
}

// Encodes the writable onto a message and returns it.
func WriteMessage(w Writable) (Message, error) {
	return BuildMessage(w.Write)
}

// Builds a message from the given builder func
func BuildMessage(fn MessageBuilder) (Message, error) {
	// initialize a new builder
	msg := newMessageBuilder()

	// invoke the builder function
	fn(msg)

	// return the results
	return msg.Build()
}

// Message builders are just functions which accept a writer.
type MessageBuilder func(w Writer)

// A simple short circuiting writer/builder.  Any error detected
// while writing automatically short circuits any future calls.
type messageBuilder struct {
	data map[string]interface{}
	err  error
}

func newMessageBuilder() *messageBuilder {
	return &messageBuilder{data: make(map[string]interface{})}
}

func (m *messageBuilder) Write(field string, value interface{}) {
	if m.err != nil {
		return
	}

	if isNil(value) {
		return
	}

	raw, err := rawValue(value)
	if err != nil {
		m.err = err
		return
	}

	m.data[field] = raw
}

func (m *messageBuilder) Build() (Message, error) {
	if m.err != nil {
		return nil, m.err
	}

	return newMessage(copyMap(m.data)), nil
}

type message struct {
	data map[string]interface{}
}

func newMessage(data map[string]interface{}) Message {
	return &message{data}
}

func (m *message) String() string {
	return fmt.Sprintf("%s", m.data)
}

func (m *message) ReadOptional(field string, target interface{}) (bool, error) {
	actual, ok := m.data[field]
	if !ok {
		return false, nil
	}

	return true, assignPointer(actual, target)
}

func (m *message) Read(field string, target interface{}) error {
	actual, ok := m.data[field]
	if !ok {
		return &MissingFieldError{field}
	}

	return assignPointer(actual, target)
}

func (m *message) Write(w Writer) {
	for k, v := range m.data {
		w.Write(k, v)
	}
}

func (m *message) Stream(e Encoder) error {
	return e.Encode(m.data)
}

// helper functions
func assignPointer(value interface{}, target interface{}) error {
	if isPrimitivePointer(target) {
		return assignPrimitivePointer(value, target)
	}

	if isMessagePointer(target) {
		return assignMessagePointer(value, target)
	}

	if isArrayPointer(target) {
		return assignArrayPointer(value, target)
	}

	return NewUnsupportedTypeError(reflect.TypeOf(target))
}

func rawValue(value interface{}) (interface{}, error) {
	if isNil(value) {
		return nil, nilValueErrorError
	}

	if isPrimitiveValue(value) {
		return value, nil
	}

	if isWritableValue(value) {
		return rawWritableValue(value)
	}

	if isArrayValue(value) {
		return rawArrayValue(value)
	}

	return nil, NewUnsupportedTypeError(reflect.TypeOf(value))
}

func isPrimitivePointer(value interface{}) bool {
	switch value.(type) {
	default:
		return false
	case *bool:
		return true
	case *string:
		return true
	case *int, *int8, *int16, *int32, *int64:
		return true
	case *uint, *uint8, *uint16, *uint32, *uint64:
		return true
	}
}

func isArrayPointer(value interface{}) bool {
	switch reflect.ValueOf(value).Elem().Kind() {
	default:
		return false
	case reflect.Array:
		return true
	case reflect.Slice:
		return true
	}
}

func isMessagePointer(value interface{}) bool {
	switch value.(type) {
	default:
		return false
	case *Message:
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

func isPrimitiveValue(value interface{}) bool {
	switch value.(type) {
	default:
		return false
	case bool:
		return true
	case string:
		return true
	case int, int8, int16, int32, int64:
		return true
	case uint, uint8, uint16, uint32, uint64:
		return true
	}
}

func isWritableValue(value interface{}) bool {
	switch value.(type) {
	default:
		return false
	case Writable:
		return true
	case Message:
		return true
	}
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

func rawWritableValue(value interface{}) (interface{}, error) {
	msg, err := WriteMessage(value.(Writable))
	if err != nil {
		return nil, err
	}
	return msg.(*message).data, nil
}

func rawArrayValue(value interface{}) (interface{}, error) {
	switch value.(type) {
	case []bool:
		return value, nil
	case []string:
		return value, nil
	case []int, []int8, []int16, []int32, []int64:
		return value, nil
	case []uint, []uint8, []uint16, []uint32, []uint64:
		return value, nil
	}

	val := reflect.ValueOf(value)
	arr := make([]interface{}, 0, val.Len())
	for i, length := 0, val.Len(); i < length; i++ {
		raw, err := rawValue(val.Index(i).Interface())
		if err != nil {
			return nil, err
		}

		arr = append(arr, raw)
	}

	return arr, nil
}

func assignMessagePointer(source interface{}, target interface{}) error {
	data, ok := source.(map[string]interface{})
	if !ok {
		return fmt.Errorf("Unable to assign source [%s] to target [%v].  Incompatible types", source, target)
	}

	reflect.ValueOf(target).Elem().Set(reflect.ValueOf(newMessage(data)))
	return nil
}

func assignArrayPointer(source interface{}, target interface{}) error {
	sourceVal := reflect.ValueOf(source)
	targetVal := reflect.ValueOf(target)

	switch typ := target.(type) {
	default:
		return NewUnsupportedTypeError(reflect.TypeOf(typ))
	case *[]bool:
		return assignGeneral(sourceVal, targetVal.Elem())
	case *[]string:
		return assignGeneral(sourceVal, targetVal.Elem())
	case *[]int, *[]int8, *[]int16, *[]int32, *[]int64:
		return assignGeneral(sourceVal, targetVal.Elem())
	case *[]uint, *[]uint8, *[]uint16, *[]uint32, *[]uint64:
		return assignGeneral(sourceVal, targetVal.Elem())
	case *[]Message:
		sourceArr, ok := source.([]interface{})
		if !ok {
			return &IncompatibleTypeError{targetVal.Type().String(), reflect.TypeOf(source).String()}
		}

		interArr := make([]Message, len(sourceArr))
		for i := 0; i < len(sourceArr); i++ {
			if err := assignMessagePointer(sourceArr[i], &interArr[i]); err != nil {
				return NewIncompatibleTypeError(targetVal.Type(), reflect.TypeOf(source))
			}
		}

		return assignGeneral(reflect.ValueOf(interArr), targetVal.Elem())
	}
}

func assignGeneral(source reflect.Value, target reflect.Value) error {
	sourceType := source.Type()
	targetType := target.Type()
	if sourceType != targetType {
		if !sourceType.ConvertibleTo(targetType) {
			return NewIncompatibleTypeError(targetType, sourceType)
		}

		source = source.Convert(targetType)
	}

	target.Set(source)
	return nil
}

func assignPrimitivePointer(source interface{}, target interface{}) error {
	ptr := reflect.ValueOf(target)
	if ptr.Kind() != reflect.Ptr || ptr.IsNil() {
		return fmt.Errorf("Unable to assign source [%v] to target [%v].  Target is not a pointer.", source, target)
	}

	return assignGeneral(reflect.ValueOf(source), ptr.Elem())
}

func copyMap(m map[string]interface{}) map[string]interface{} {
	ret := make(map[string]interface{})
	for k, v := range m {
		ret[k] = v
	}
	return ret
}

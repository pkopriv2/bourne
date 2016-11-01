package enc

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/pkg/errors"
)

// To be returned with a nil value is erroneously detected.
var nilValueErrorError = errors.New("Nil value")


// Builds a message from the given builder func
func Build(fn WriteFn) Message {
	// initialize a new builder
	msg := newMessageBuilder()

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
func StreamWrite(enc Encoder, w Writable) error {
	return Write(w).Stream(enc)
}

// Decodes a message from the stream.
func StreamRead(e Decoder) (Message, error) {
	var raw map[string]interface{}
	if err := e.Decode(&raw); err != nil {
		return nil, err
	}

	return newMessage(raw), nil
}


// Message builders are just functions which accept a writer.
type WriteFn func(w Writer)

// A simple short circuiting writer/builder.  Any error detected
// while writing automatically short circuits any future calls.
type messageBuilder struct {
	data map[string]interface{}
}

func newMessageBuilder() *messageBuilder {
	return &messageBuilder{data: make(map[string]interface{})}
}

func (m *messageBuilder) Write(field string, v interface{}) {
	if isNil(v) {
		return
	}

	switch val := v.(type) {
	default:
		panic(errors.Errorf("Unable to write field [%v]. Unsupprted type %v", field, reflect.TypeOf(val)))
	case string:
		m.data[field] = val
	case bool:
		m.data[field] = val
	case int:
		m.Write(field, strconv.Itoa(val))
	case Writable:
		m.data[field] = Write(val).(*message).data
	case []string:
		m.data[field] = val
	case []bool:
		m.data[field] = val
	case []int:
		str := make([]string, 0, len(val))
		for _,i := range val {
			str = append(str, strconv.Itoa(i))
		}
		m.Write(field, str)
	case []Writable:
		writables := make([]map[string]interface{}, 0, len(val))
		for _,i := range val {
			writables = append(writables, Write(i).(*message).data)
		}
		m.Write(field, writables)
	}
}

func (m *messageBuilder) Build() Message {
	return newMessage(copyMap(m.data))
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

func (m *message) ReadOptional(field string, raw interface{}) (bool, error) {
	if err := m.Read(field, raw); err != nil {
		if _, ok := err.(*MissingFieldError); ok {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (m *message) Read(field string, raw interface{}) error {
	data, ok := m.data[field]
	if !ok {
		return &MissingFieldError{field}
	}

	var err error
	switch ptr := raw.(type) {
	default:
		panic(errors.Errorf("Unable to"))
	case *string:
		err = ReadString(data, ptr)
	case *bool:
		err = ReadBool(data, ptr)
	case *int:
		err = ReadInt(data, ptr)
	case *Reader:
		err = ReadReader(data, ptr)
	case *[]string:
		err = ReadStrings(data, ptr)
	case *[]bool:
		err = ReadBools(data, ptr)
	case *[]int:
		err = ReadInts(data, ptr)
	case *[]Reader:
		err = ReadReaders(data, ptr)
	}

	if err != nil {
		err = errors.Wrapf(err, "Error while reading field [%v]", field)
	}

	return err
}

func (m *message) Write(w Writer) {
	for k,v := range m.data {
		w.Write(k, v)
	}
}

func (m *message) Stream(e Encoder) error {
	return e.Encode(m.data)
}

func ReadString(data interface{}, ptr *string) error {
	str, ok := data.(string)
	if !ok {
		return NewIncompatibleTypeError(data, ptr)
	}

	*ptr = str
	return nil
}

func ReadBool(data interface{}, ptr *bool) error {
	str, ok := data.(bool)
	if !ok {
		return NewIncompatibleTypeError(data, ptr)
	}

	*ptr = str
	return nil
}

func ReadInt(data interface{}, ptr *int) error {
	var str string
	if err := ReadString(data, &str); err != nil {
		return err
	}

	val, err := strconv.Atoi(str)
	if err != nil {
		return err
	}

	*ptr = val
	return nil
}

func ReadReader(data interface{}, ptr *Reader) error {
	raw, ok := data.(map[string]interface{})
	if !ok {
		return NewIncompatibleTypeError(reflect.TypeOf(*ptr), reflect.TypeOf(data))
	}

	*ptr = newMessage(raw)
	return nil
}

func ReadStrings(data interface{}, ptr *[]string) error {
	val, ok := data.([]string)
	if !ok {
		return NewIncompatibleTypeError(data, ptr)
	}

	*ptr = val
	return nil
}

func ReadBools(data interface{}, ptr *[]bool) error {
	val, ok := data.([]bool)
	if !ok {
		return NewIncompatibleTypeError(data, ptr)
	}

	*ptr = val
	return nil
}

func ReadInts(data interface{}, ptr *[]int) error {
	var strs []string
	if err := ReadStrings(data, &strs); err != nil {
		return err
	}

	ret := make([]int, 0, len(strs))
	for _,str := range strs {
		val, err := strconv.Atoi(str)
		if err != nil {
			return err
		}

		ret = append(ret, val)
	}

	*ptr = ret
	return nil
}

func ReadReaders(data interface{}, ptr *[]Reader) error {
	raw, ok := data.([]map[string]interface{})
	if !ok {
		return NewIncompatibleTypeError(reflect.TypeOf(*ptr), reflect.TypeOf(data))
	}

	val := make([]Reader, 0, len(raw))
	for _,v := range raw {
		val = append(val, newMessage(v))
	}

	*ptr = val
	return nil
}

// helper functions
func copyMap(m map[string]interface{}) map[string]interface{} {
	ret := make(map[string]interface{})
	for k, v := range m {
		ret[k] = v
	}
	return ret
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

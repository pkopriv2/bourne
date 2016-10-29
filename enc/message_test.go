package enc

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildMessage_Nil(t *testing.T) {
	var m Message
	msg, _ := BuildMessage(func(w Writer) {
		w.Write("field", m)
	})

	var field Message
	err, ok := msg.Read("field", &field).(*MissingFieldError)
	assert.Error(t, err)
	assert.True(t, ok)
}

func TestBuildMessage_Bool(t *testing.T) {
	msg, _ := BuildMessage(func(w Writer) {
		w.Write("field", true)
	})

	var field bool
	assert.Nil(t, msg.Read("field", &field))
	assert.Equal(t, true, field)
}

func TestBuildMessage_Int(t *testing.T) {
	msg, _ := BuildMessage(func(w Writer) {
		w.Write("field", int(1))
	})

	var field int
	assert.Nil(t, msg.Read("field", &field))
	assert.Equal(t, int(1), field)
}

func TestBuildMessage_Uint(t *testing.T) {
	msg, _ := BuildMessage(func(w Writer) {
		w.Write("field", uint(1))
	})

	var field uint
	assert.Nil(t, msg.Read("field", &field))
	assert.Equal(t, uint(1), field)
}

func TestBuildMessage_String(t *testing.T) {
	msg, _ := BuildMessage(func(w Writer) {
		w.Write("field", "string")
	})

	var field string
	assert.Nil(t, msg.Read("field", &field))
	assert.Equal(t, "string", field)
}

func TestBuildMessage_Writable(t *testing.T) {
	msg1, _ := BuildMessage(func(w Writer) {
		w.Write("field", "string")
	})

	msg2, err := BuildMessage(func(w Writer) {
		w.Write("field", msg1)
	})
	assert.Nil(t, err)

	var field Message
	assert.Nil(t, msg2.Read("field", &field))
	assert.Equal(t, msg1, field)
}

func TestBuildMessage_Int_Array(t *testing.T) {
	msg, err := BuildMessage(func(w Writer) {
		w.Write("field", []int{1})
	})
	assert.Nil(t, err)

	var field []int
	assert.Nil(t, msg.Read("field", &field))
	assert.Equal(t, []int{1}, field)
}

func TestBuildMessage_Bool_Array(t *testing.T) {
	msg, err := BuildMessage(func(w Writer) {
		w.Write("field", []bool{true, true, false})
	})
	assert.Nil(t, err)

	var field []bool
	assert.Nil(t, msg.Read("field", &field))
	assert.Equal(t, []bool{true, true, false}, field)
}

func TestBuildMessage_String_Array(t *testing.T) {
	arr := []string{"hello", "world"}
	msg, err := BuildMessage(func(w Writer) {
		w.Write("field", arr)
	})

	assert.Nil(t, err)

	var field []string
	assert.Nil(t, msg.Read("field", &field))
	assert.Equal(t, arr, field)
}

func TestBuildMessage_Message_Array(t *testing.T) {
	msg1, err1 := BuildMessage(func(w Writer) {
		w.Write("field1", "value1")
	})

	msg2, err2 := BuildMessage(func(w Writer) {
		w.Write("field2", "value2")
	})

	msg3, err3 := BuildMessage(func(w Writer) {
		w.Write("field3", "value3")
	})

	msg, err := BuildMessage(func(w Writer) {
		w.Write("field", []Writable{msg1, msg2, msg3})
	})

	assert.Nil(t, err1)
	assert.Nil(t, err2)
	assert.Nil(t, err3)
	assert.Nil(t, err)

	arr := []Message{msg1, msg2, msg3}

	var field []Message
	assert.Nil(t, msg.Read("field", &field))
	assert.Equal(t, arr, field)
}

func TestEncode_JSON_Simple(t *testing.T) {

	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	dec := json.NewDecoder(buf)

	val := &TestWritable{1, 2, 3, 4, "5"}
	err := EncodeWritable(enc, val)
	assert.Nil(t, err)

	msg, err := DecodeMessage(dec)
	assert.Nil(t, err)

	actual, err := ParseTestWritable(msg)
	assert.Equal(t, val, actual)
}

func TestEncode_GOB_Simple(t *testing.T) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	dec := gob.NewDecoder(buf)

	val := &TestWritable{1, 2, 3, 4, "5"}
	err := EncodeWritable(enc, val)
	assert.Nil(t, err)

	msg, err := DecodeMessage(dec)
	assert.Nil(t, err)

	actual, err := ParseTestWritable(msg)
	assert.Equal(t, val, actual)
}

func TestEncode_JSON_Complex(t *testing.T) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	dec := json.NewDecoder(buf)

	val := &TestWritableComplex{1, &TestWritable{1, 2, 3, 4, "5"}}
	err := EncodeWritable(enc, val)
	assert.Nil(t, err)

	msg, err := DecodeMessage(dec)
	assert.Nil(t, err)

	actual, err := ParseTestWritableComplex(msg)
	assert.Equal(t, val, actual)
}

func TestEncode_JSON_Complex_Nil(t *testing.T) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	dec := json.NewDecoder(buf)

	val := &TestWritableComplex{field1: 1}
	err := EncodeWritable(enc, val)
	assert.Nil(t, err)

	msg, err := DecodeMessage(dec)
	assert.Nil(t, err)

	fmt.Println(msg)
	actual, err := ParseTestWritableComplex(msg)
	assert.Nil(t, err)
	assert.Equal(t, val, actual)
}

// TODO: Test these guys!
type TestWritable struct {
	field1 int
	field2 int8
	field3 int16
	field4 int32
	field5 string
}

func (t *TestWritable) Write(e Writer) {
	e.Write("field1", t.field1)
	e.Write("field2", t.field2)
	e.Write("field3", t.field3)
	e.Write("field4", t.field4)
	e.Write("field5", t.field5)
}

func ParseTestWritable(m Reader) (*TestWritable, error) {
	var ret TestWritable

	if err := m.Read("field1", &ret.field1); err != nil {
		return nil, err
	}

	if err := m.Read("field2", &ret.field2); err != nil {
		return nil, err
	}

	if err := m.Read("field3", &ret.field3); err != nil {
		return nil, err
	}

	if err := m.Read("field4", &ret.field4); err != nil {
		return nil, err
	}

	if err := m.Read("field5", &ret.field5); err != nil {
		return nil, err
	}

	return &ret, nil
}

type TestWritableComplex struct {
	field1 int
	field2 *TestWritable // nillable
}

func (t *TestWritableComplex) Write(e Writer) {
	e.Write("field1", t.field1)
	e.Write("field2", t.field2)
}

func ParseTestWritableComplex(m Reader) (*TestWritableComplex, error) {
	var ret TestWritableComplex

	if err := m.Read("field1", &ret.field1); err != nil {
		return nil, err
	}

	var msg Message
	if err := m.Read("field2", &msg); err == nil {
		writable, err := ParseTestWritable(msg)
		if err != nil {
			return nil, err
		}

		ret.field2 = writable
	}

	return &ret, nil
}

package enc

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRead_Empty(t *testing.T) {
	msg := Build(func(w Writer) {
	})

	var field string
	assert.Error(t, msg.Read("field", &field))
}

func TestReadOptional_Empty(t *testing.T) {
	msg := Build(func(w Writer) {
	})

	var field string
	ok, err := msg.ReadOptional("field", &field)
	assert.False(t, ok)
	assert.Nil(t, err)
}

func TestBuild_String(t *testing.T) {
	val := "hello, world"
	msg := Build(func(w Writer) {
		w.Write("field", val)
	})

	var field string
	assert.Nil(t, msg.Read("field", &field))
	assert.Equal(t, val, field)
}

func TestBuild_Bool(t *testing.T) {
	msg := Build(func(w Writer) {
		w.Write("field", true)
	})

	var field bool
	assert.Nil(t, msg.Read("field", &field))
	assert.Equal(t, true, field)
}

func TestRead_Composite(t *testing.T) {
	sub := Build(func(w Writer) {
		w.Write("field1", true)
		w.Write("field2", "hello")
	})

	msg := Build(func(w Writer) {
		w.Write("field", sub)
	})

	var actual Message
	assert.Nil(t, msg.Read("field", &actual))
	assert.Equal(t, sub, actual)
}

func TestRead_Composite_Array(t *testing.T) {
	sub1 := Build(func(w Writer) {
		w.Write("field1", true)
	})
	sub2 := Build(func(w Writer) {
		w.Write("field1", true)
	})

	val := []Message{sub1, sub2}
	msg := Build(func(w Writer) {
		w.Write("field", val)
	})

	var actual []Message
	assert.Nil(t, msg.Read("field", &actual))
	assert.Equal(t, val, actual)
}

func TestEncode_JSON_String(t *testing.T) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	dec := json.NewDecoder(buf)

	msg := Build(func(w Writer) {
		w.Write("field", "hello, world")
	})

	err := Encode(enc, msg)
	assert.Nil(t, err)

	actual, err := Decode(dec)
	assert.Nil(t, err)

	assert.Equal(t, msg, actual)
}

func TestEncode_JSON_Bool(t *testing.T) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	dec := json.NewDecoder(buf)

	msg := Build(func(w Writer) {
		w.Write("field", true)
	})

	err := Encode(enc, msg)
	assert.Nil(t, err)

	actual, err := Decode(dec)
	assert.Nil(t, err)

	assert.Equal(t, msg, actual)
}

func TestEncode_JSON_Composite(t *testing.T) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	dec := json.NewDecoder(buf)

	msg1 := Build(func(w Writer) {
		w.Write("field", true)
		w.Write("field2", "hello, world")
	})

	msg2 := Build(func(w Writer) {
		w.Write("field", msg1)
	})

	err := Encode(enc, msg2)
	assert.Nil(t, err)

	actual, err := Decode(dec)
	assert.Nil(t, err)

	assert.Equal(t, msg2, actual)
}

func TestBuild_Strings(t *testing.T) {
	val := []string{"hello", "world"}
	msg := Build(func(w Writer) {
		w.Write("field", val)
	})

	var field []string
	assert.Nil(t, msg.Read("field", &field))
	assert.Equal(t, val, field)
}

func TestBuild_Bools(t *testing.T) {
	val := []bool{true, false, true}
	msg := Build(func(w Writer) {
		w.Write("field", val)
	})

	var field []bool
	assert.Nil(t, msg.Read("field", &field))
	assert.Equal(t, val, field)
}

func TestBuild_Strings_Composite(t *testing.T) {
	sub := Build(func(w Writer) {
		w.Write("field", []string{"hello", "world"})
	})

	msg := Build(func(w Writer) {
		w.Write("field", sub)
	})

	var field Message
	assert.Nil(t, msg.Read("field", &field))
	assert.Equal(t, sub, field)
}

func TestEncode_JSON_Strings(t *testing.T) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	dec := json.NewDecoder(buf)

	msg := Build(func(w Writer) {
		w.Write("field", []string{"hello", "world"})
	})

	err := Encode(enc, msg)
	assert.Nil(t, err)

	actual, err := Decode(dec)
	assert.Nil(t, err)

	assert.Equal(t, msg, actual)
}

func TestEncode_GOB_Strings(t *testing.T) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	dec := gob.NewDecoder(buf)

	msg := Build(func(w Writer) {
		w.Write("field", []string{"hello", "world"})
	})

	err := Encode(enc, msg)
	assert.Nil(t, err)

	actual, err := Decode(dec)
	assert.Nil(t, err)

	assert.Equal(t, msg, actual)
}

func TestBuild_Int(t *testing.T) {
	msg := Build(func(w Writer) {
		w.Write("field", int(1))
	})

	var field int
	assert.Nil(t, msg.Read("field", &field))
	assert.Equal(t, int(1), field)
}

func TestBuild_Ints(t *testing.T) {
	val := []int{1, 2, 3}
	msg := Build(func(w Writer) {
		w.Write("field", val)
	})

	var field []int
	assert.Nil(t, msg.Read("field", &field))
	assert.Equal(t, val, field)
}

func TestEncode_JSON_Complex(t *testing.T) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	dec := json.NewDecoder(buf)

	val := &TestWritableComplex{1, &TestWritable{1, "2"}}
	err := Encode(enc, val)
	assert.Nil(t, err)

	msg, err := Decode(dec)
	assert.Nil(t, err)

	actual, err := ParseTestWritableComplex(msg)
	assert.Nil(t, err)
	assert.Equal(t, val, actual)
}

func TestEncode_JSON_Complex_Nil(t *testing.T) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	dec := json.NewDecoder(buf)

	val := &TestWritableComplex{field1: 1}
	err := Encode(enc, val)
	assert.Nil(t, err)

	msg, err := Decode(dec)
	assert.Nil(t, err)

	actual, err := ParseTestWritableComplex(msg)
	assert.Nil(t, err)
	assert.Equal(t, val, actual)
}

type TestWritable struct {
	field1 int
	field2 string
}

func (t *TestWritable) Write(e Writer) {
	e.Write("field1", t.field1)
	e.Write("field2", t.field2)
}

func ParseTestWritable(m Reader) (*TestWritable, error) {
	var ret TestWritable

	if err := m.Read("field1", &ret.field1); err != nil {
		return nil, err
	}

	if err := m.Read("field2", &ret.field2); err != nil {
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

	var msg Reader
	if _, err := m.ReadOptional("field2", &msg); err != nil {
		return nil, err
	}

	if msg != nil {
		val, err := ParseTestWritable(msg)
		if err != nil {
			return nil, err
		}

		ret.field2 = val
	}

	return &ret, nil
}

package scribe

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadBool_Empty(t *testing.T) {
	msg := Build(func(w Writer) {})

	var field bool
	assert.Error(t, msg.ReadBool("field", &field))
}

func TestReadBools_Empty(t *testing.T) {
	msg := Build(func(w Writer) {})

	var field []bool
	assert.Error(t, msg.ReadBools("field", &field))
}

func TestReadString_Empty(t *testing.T) {
	msg := Build(func(w Writer) {})

	var field string
	assert.Error(t, msg.ReadString("field", &field))
}

func TestReadStrings_Empty(t *testing.T) {
	msg := Build(func(w Writer) {})

	var field []string
	assert.Error(t, msg.ReadStrings("field", &field))
}

func TestReadMessage_Empty(t *testing.T) {
	msg := Build(func(w Writer) {})

	var field Message
	assert.Error(t, msg.ReadMessage("field", &field))
}

func TestReadMessages_Empty(t *testing.T) {
	msg := Build(func(w Writer) {})

	var field []Message
	assert.Error(t, msg.ReadMessages("field", &field))
}

func TestBool_ReadWrite(t *testing.T) {
	exp := true
	msg := Build(func(w Writer) {
		w.WriteBool("field", exp)
	})

	var val bool
	err := msg.ReadBool("field", &val)
	assert.Nil(t, err)
	assert.Equal(t, exp, val)
}

func TestBools_ReadWrite(t *testing.T) {
	exp := []bool{true, false}
	msg := Build(func(w Writer) {
		w.WriteBools("field", exp)
	})

	var val []bool
	err := msg.ReadBools("field", &val)
	assert.Nil(t, err)
	assert.Equal(t, exp, val)
}

func TestString_ReadWrite(t *testing.T) {
	exp := "hello,world"
	msg := Build(func(w Writer) {
		w.WriteString("field", exp)
	})

	var val string
	err := msg.ReadString("field", &val)
	assert.Nil(t, err)
	assert.Equal(t, exp, val)
}

func TestStrings_ReadWrite(t *testing.T) {
	exp := []string{"hello", "world"}
	msg := Build(func(w Writer) {
		w.WriteStrings("field", exp)
	})

	var val []string
	err := msg.ReadStrings("field", &val)
	assert.Nil(t, err)
	assert.Equal(t, exp, val)
}

func TestMessage_ReadWrite(t *testing.T) {
	exp := Build(func(w Writer) {
		w.WriteString("field", "hello, world")
	})

	msg := Build(func(w Writer) {
		w.WriteMessage("field", exp)
	})

	var val Message
	err := msg.ReadMessage("field", &val)
	assert.Nil(t, err)
	assert.Equal(t, exp, val)
}

func TestMessages_ReadWrite(t *testing.T) {
	msg1 := Build(func(w Writer) {
		w.WriteString("field", "hello")
	})
	msg2 := Build(func(w Writer) {
		w.WriteString("field", "world")
	})

	exp := []Message{msg1, msg2}
	msg := Build(func(w Writer) {
		w.WriteMessages("field", exp)
	})

	var val []Message
	err := msg.ReadMessages("field", &val)
	assert.Nil(t, err)
	assert.Equal(t, exp, val)
}

func TestMessages_Write_InvalidType(t *testing.T) {
	assert.Panics(t, func() {
		Build(func(w Writer) {
			w.WriteMessages("field", "hello")
		})
	})
}

func TestEncode_JSON_String(t *testing.T) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	dec := json.NewDecoder(buf)

	msg := Build(func(w Writer) {
		w.WriteString("field", "hello, world")
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
		w.WriteBool("field", true)
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
		w.WriteBool("field", true)
		w.WriteString("field2", "hello, world")
	})

	msg2 := Build(func(w Writer) {
		w.WriteMessage("field", msg1)
	})

	err := Encode(enc, msg2)
	assert.Nil(t, err)

	actual, err := Decode(dec)
	assert.Nil(t, err)

	assert.Equal(t, msg2, actual)
}

func TestEncode_GOB_Strings(t *testing.T) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	dec := gob.NewDecoder(buf)

	msg := Build(func(w Writer) {
		w.WriteStrings("field", []string{"hello", "world"})
	})

	err := Encode(enc, msg)
	assert.Nil(t, err)

	actual, err := Decode(dec)
	assert.Nil(t, err)

	assert.Equal(t, msg, actual)
}

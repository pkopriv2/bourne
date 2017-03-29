package scribe

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"math/big"
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

func TestReadOptionalMessage_Empty(t *testing.T) {
	msg := Build(func(w Writer) {})

	var field Message
	err := msg.ReadOptionalMessage("field", &field)

	assert.Nil(t, field)
	assert.Nil(t, err)
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

func TestBigInt_ReadWrite(t *testing.T) {
	exp := big.NewInt(100)
	msg := Build(func(w Writer) {
		w.WriteBigInt("field", exp)
	})

	var val *big.Int
	err := msg.ReadBigInt("field", &val)
	assert.Nil(t, err)
	assert.Equal(t, exp, val)
}

func TestBigInts_ReadWrite(t *testing.T) {
	exp := []*big.Int{big.NewInt(1), big.NewInt(1<<63-1)}
	msg := Build(func(w Writer) {
		w.WriteBigInts("field", exp)
	})

	var val []*big.Int
	err := msg.ReadBigInts("field", &val)
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

type embedded struct {
	val int
}

func (e embedded) Write(w Writer) {
	w.WriteInt("val", e.val)
}

func embeddedParser(r Reader) (interface{}, error) {
	var ret embedded
	err := r.ReadInt("val", &ret.val)
	return ret, err
}

func TestParseMessage(t *testing.T) {
	msg := Build(func(w Writer) {
		w.WriteMessage("embedded", embedded{2})
	})

	var embed embedded
	assert.Nil(t, msg.ParseMessage("embedded", &embed, embeddedParser))
	assert.Equal(t, embedded{2}, embed)
}

func TestParseMessages(t *testing.T) {
	msg := Build(func(w Writer) {
		w.WriteMessages("embedded", []embedded{embedded{2}, embedded{1}})
	})

	var embed []embedded
	assert.Nil(t, msg.ParseMessages("embedded", &embed, embeddedParser))
	assert.Equal(t, []embedded{embedded{2}, embedded{1}}, embed)
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

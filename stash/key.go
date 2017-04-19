package stash

import (
	"bytes"
	"encoding/binary"

	uuid "github.com/satori/go.uuid"
)

type Key []byte

func (k Key) Child(child []byte) Key {
	return Key(append([]byte(k), child...))
}

func (k Key) ChildString(child string) Key {
	return k.Child([]byte(child))
}

func (k Key) ChildInt(child int) Key {
	return k.Child(IntBytes(child))
}

func (k Key) ChildUUID(child uuid.UUID) Key {
	return k.Child(child.Bytes())
}

func (k Key) Equals(other []byte) bool {
	return bytes.Equal(k.Raw(), other)
}

func (k Key) Compare(other []byte) int {
	return bytes.Compare(k.Raw(), other)
}

func (k Key) ParentOf(child []byte) bool {
	return bytes.HasPrefix(child, k)
}

func (k Key) ChildOf(parent []byte) bool {
	return bytes.HasPrefix(k, parent)
}

func (k Key) Raw() []byte {
	return []byte(k)
}

func String(key string) Key {
	return Key([]byte(key))
}

func UUID(key uuid.UUID) Key {
	return Key(key.Bytes())
}

func ParseUUID(val []byte) (uuid.UUID, error) {
	return uuid.FromBytes(val)
}

func Int(key int) Key {
	return Key(IntBytes(key))
}

func IntBytes(val int) []byte {
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, int64(val))
	return buf.Bytes()
}


func ParseInt(val []byte) (ret int, err error) {
	var raw int64
	buf := bytes.NewBuffer(val)
	err = binary.Read(buf, binary.BigEndian, &raw)
	ret = int(raw)
	return
}

func Bool(key bool) Key {
	return Key(BoolBytes(key))
}

func BoolBytes(val bool) []byte {
	buf := &bytes.Buffer{}
	if val {
		binary.Write(buf, binary.BigEndian, uint8(1))
	} else {
		binary.Write(buf, binary.BigEndian, uint8(0))
	}
	return buf.Bytes()
}

func ParseBool(val []byte) (ret bool, err error) {
	var raw uint8
	buf := bytes.NewBuffer(val)
	err = binary.Read(buf, binary.BigEndian, &raw)
	ret = raw == 1
	return
}


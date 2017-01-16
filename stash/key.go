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

func (k Key) Raw() []byte {
	return []byte(k)
}

func UUID(key uuid.UUID) Key {
	return Key(key.Bytes())
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
	buf := bytes.NewBuffer(val)
	err = binary.Read(buf, binary.BigEndian, &ret)
	return
}

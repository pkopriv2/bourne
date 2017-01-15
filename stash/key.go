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

func (k Key) Raw() []byte {
	return []byte(k)
}

// type CompositeKey []byte
//
// func (k CompositeKey) Child(child []byte) CompositeKey {
	// return CompositeKey(append([]byte(k), child...))
// }
//
// func (k CompositeKey) ChildInt(child int) CompositeKey {
	// return k.Child(IntBytes(child))
// }
//
// func (k CompositeKey) ChildUUID(child uuid.UUID) CompositeKey {
	// return k.Child(child.Bytes())
// }
//
// func (k CompositeKey) Raw() []byte {
	// return []byte(k)
// }

func NewUUIDKey(key uuid.UUID) Key {
	return Key(key.Bytes())
}

func NewIntKey(key int) Key {
	return Key(IntBytes(key))
}

func IntBytes(val int) []byte {
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, uint32(val))
	return buf.Bytes()
}

func ParseInt(val []byte) (ret int, err error) {
	buf := bytes.NewBuffer(val)
	err = binary.Read(buf, binary.BigEndian, &ret)
	return
}

package scribe

import uuid "github.com/satori/go.uuid"

type IntMessage int

func (i IntMessage) Write(w Writer) {
	w.WriteInt("val", int(i))
}

func ReadIntMessage(r Reader) (val int, err error) {
	err = r.ReadInt("val", &val)
	return
}

func ParseIntMessage(r Reader) (interface{}, error) {
	return ReadIntMessage(r)
}

func NewIntMessage(val int) Message {
	return Write(IntMessage(val))
}

type BoolMessage bool

func (i BoolMessage) Write(w Writer) {
	w.WriteBool("val", bool(i))
}

func ReadBoolMessage(r Reader) (val bool, err error) {
	err = r.ReadBool("val", &val)
	return
}

func ParseBoolMessage(r Reader) (interface{}, error) {
	return ReadBoolMessage(r)
}

func NewBoolMessage(val bool) Message {
	return Write(BoolMessage(val))
}

type UUIDMessage uuid.UUID

func (i UUIDMessage) Write(w Writer) {
	w.WriteUUID("val", uuid.UUID(i))
}

func ReadUUIDMessage(r Reader) (val uuid.UUID, err error) {
	err = r.ReadUUID("val", &val)
	return
}

func ParseUUIDMessage(r Reader) (interface{}, error) {
	return ReadUUIDMessage(r)
}

func NewUUIDMessage(val uuid.UUID) Message {
	return Write(UUIDMessage(val))
}

package scribe

import (
	"encoding/base64"
	"strconv"

	uuid "github.com/satori/go.uuid"
)

func ReadMessages(r Reader, field string) (ret []Message, err error) {
	err = r.Read(field, &ret)
	return
}

// Adding basic binary support to scribe.
func WriteBytes(w Writer, field string, val []byte) {
	w.Write(field, base64.StdEncoding.EncodeToString(val))
}

func ReadBytes(r Reader, field string) ([]byte, error) {
	var value string
	if err := r.Read(field, &value); err != nil {
		return nil, err
	}

	return base64.StdEncoding.DecodeString(value)
}

// Adding integer support to scribe.
func WriteInt(w Writer, field string, val int) {
	w.Write(field, strconv.Itoa(val))
}

func ReadInt(r Reader, field string) (int, error) {
	var value string
	if err := r.Read(field, &value); err != nil {
		return 0, err
	}

	return strconv.Atoi(value)
}


// Adding uuid support to scribe.
func WriteUUID(w Writer, field string, val uuid.UUID) {
	w.Write(field, val.String())
}

func ReadUUID(r Reader, field string) (uuid.UUID, error) {
	var value string
	if err := r.Read(field, &value); err != nil {
		return *new(uuid.UUID), err
	}

	return uuid.FromString(value)
}

package scribe

import uuid "github.com/satori/go.uuid"

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

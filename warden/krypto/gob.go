package krypto

import (
	"bytes"
	"encoding/gob"
)

func gobBytes(v interface{}) (ret []byte, err error) {
	var buf bytes.Buffer
	err = gob.NewEncoder(&buf).Encode(v)
	ret = buf.Bytes()
	return
}

func parseGobBytes(raw []byte, v interface{}) (bool, error) {
	if raw == nil {
		return false, nil
	}
	return true, gob.NewDecoder(bytes.NewBuffer(raw)).Decode(v)
}

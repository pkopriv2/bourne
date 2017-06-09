package krypto

import (
	"bytes"
	"encoding/gob"

	"github.com/pkg/errors"
)

// This is currently VERY inefficient!
var (
	Gob = GobEncoder{}
)

type GobEncoder struct{}

func (g *GobEncoder) Encode(v interface{}) ([]byte, error) {
	ret, err := gobBytes(v)
	return ret, errors.WithStack(err)
}

func (g *GobEncoder) Decode(raw []byte, v interface{}) error {
	_, err := parseGobBytes(raw, &v)
	return errors.WithStack(err)
}

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

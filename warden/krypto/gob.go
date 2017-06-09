package krypto

import (
	"bytes"
	"encoding/gob"

	"github.com/pkg/errors"
)

var (
	Gob = &GobEncoder{}
)

type GobEncoder struct{}

func (g *GobEncoder) Protocol() string {
	return "Gob/1.0.0"
}

func (g *GobEncoder) Encode(v interface{}) (Encoding, error) {
	body, err := gobBytes(v)
	if err != nil {
		return Encoding{}, errors.WithStack(err)
	}
	return Encoding{g.Protocol(), body}, nil
}

func (g *GobEncoder) Decode(raw Encoding, v interface{}) error {
	_, err := parseGobBytes(raw.Body, v)
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

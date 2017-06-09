package krypto

import (
	"bytes"
	"encoding/json"

	"github.com/pkg/errors"
)

var (
	Json = &JsonEncoder{}
)

type JsonEncoder struct{}

func (g *JsonEncoder) Protocol() string {
	return "Json/1.0.0"
}

func (g *JsonEncoder) Encode(v interface{}) (Encoding, error) {
	body, err := jsonBytes(v)
	if err != nil {
		return Encoding{}, errors.WithStack(err)
	}
	return Encoding{g.Protocol(), body}, nil
}

func (g *JsonEncoder) Decode(raw Encoding, v interface{}) error {
	_, err := parseJsonBytes(raw.Body, v)
	return errors.WithStack(err)
}

func jsonBytes(v interface{}) (ret []byte, err error) {
	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(v)
	ret = buf.Bytes()
	return
}

func parseJsonBytes(raw []byte, v interface{}) (bool, error) {
	if raw == nil {
		return false, nil
	}
	return true, json.NewDecoder(bytes.NewBuffer(raw)).Decode(v)
}

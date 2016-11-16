package amoeba

import (
	"bytes"
	"strings"
)

// An integer key type
type IntKey int

func (i IntKey) Compare(s Sortable) int {
	return int(i - s.(IntKey))
}

// A string key type
type StringKey string

func (i StringKey) Compare(s Sortable) int {
	return strings.Compare(string(i), string(s.(StringKey)))
}

// A byte array key type.
type BytesKey []byte

func (i BytesKey) Compare(s Sortable) int {
	return bytes.Compare([]byte(i), []byte(s.(BytesKey)))
}

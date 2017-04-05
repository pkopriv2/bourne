package warden

import (
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"hash"

	"golang.org/x/crypto/pbkdf2"
)

// Useful cryptographic binary functions.
type Bytes []byte

// Zeroes the underlying byte array.  (Useful for deleting secret information)
func (b Bytes) Destroy() {
	for i := 0; i < len(b); i++ {
		b[i] = 0
	}
}

// Returns the length of the underlying array
func (b Bytes) Size() int {
	return len(b)
}

// Returns a base64 representation of the array
func (b Bytes) Base64() string {
	return base64.StdEncoding.EncodeToString(b)
}

// Returns a base32 representation of the array
func (b Bytes) Base32() string {
	return base32.StdEncoding.EncodeToString(b)
}

// Returns a base32 representation of the array
func (b Bytes) Pem(header string) string {
	blk := &pem.Block{Type: header, Bytes: b}
	return string(pem.EncodeToMemory(blk))
}


func (b Bytes) Hex() string {
	return hex.EncodeToString(b)
}

// Returns a string representation of the array
func (b Bytes) String() string {
	base64 := b.Base64()

	if b.Size() <= 32 {
		return base64
	} else {
		return fmt.Sprintf("%v... (total=%v)", base64[:32], len(base64))
	}
}

// Returns a new byte array for use as a cipher key
func (b Bytes) Hash(h Hash) (Bytes, error) {
	return h.Hash(b)
}

// Returns a new byte array for use as a cipher key
func (b Bytes) Pbkdf2(salt []byte, iter int, size int, h hash.Hash) Bytes {
	return pbkdf2.Key(b, salt, iter, size, func() hash.Hash {
		return h
	})
}

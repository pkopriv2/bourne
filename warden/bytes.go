package warden

import (
	"bytes"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"hash"

	"golang.org/x/crypto/pbkdf2"
)

// Zeroest the input array of bytes
func destroyBytes(buf []byte) {
	cryptoBytes(buf).Destroy()
}

func hashN(bytes []byte, h Hash, n int) (ret cryptoBytes, err error) {
	ret = bytes
	for i := 0; i<n; i++ {
		ret, err = ret.Hash(h)
	}
	return
}

// Useful cryptographic binary functions.
type cryptoBytes []byte

// Zeroes the underlying byte array.  (Useful for deleting secret information)
func (b cryptoBytes) Destroy() {
	for i := 0; i < len(b); i++ {
		b[i] = 0
	}
}

// Zeroes the underlying byte array.  (Useful for deleting secret information)
func (b cryptoBytes) Equals(other cryptoBytes) bool {
	return bytes.Equal(b, other)
}

// Returns the length of the underlying array
func (b cryptoBytes) Size() int {
	return len(b)
}

// Returns a base64 representation of the array
func (b cryptoBytes) Base64() string {
	return base64.StdEncoding.EncodeToString(b)
}

// Returns a base32 representation of the array
func (b cryptoBytes) Base32() string {
	return base32.StdEncoding.EncodeToString(b)
}

// Returns a pem representation of the array
func (b cryptoBytes) Pem(header string) string {
	blk := &pem.Block{Type: header, Bytes: b}
	return string(pem.EncodeToMemory(blk))
}

// Returns a hex representation of the array
func (b cryptoBytes) Hex() string {
	return hex.EncodeToString(b)
}

// Returns a string representation of the array
func (b cryptoBytes) String() string {
	base64 := b.Base64()

	if b.Size() <= 32 {
		return base64
	} else {
		return fmt.Sprintf("%v... (total=%v)", base64[:32], len(base64))
	}
}

// Returns the hash of the byte array.
func (b cryptoBytes) Hash(h Hash) (cryptoBytes, error) {
	return h.Hash(b)
}

// Returns a new byte array for use as a cipher key
func (b cryptoBytes) Pbkdf2(salt []byte, iter int, size int, h hash.Hash) cryptoBytes {
	return pbkdf2.Key(b, salt, iter, size, func() hash.Hash {
		return h
	})
}

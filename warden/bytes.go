package warden

import (
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"

	"golang.org/x/crypto/pbkdf2"
)

// Useful cryptographic binary functions.
type crypoBytes []byte

// Zeroes the underlying byte array.  (Useful for deleting secret information)
func (b crypoBytes) Destroy() {
	for i := 0; i < len(b); i++ {
		b[i] = 0
	}
}

// Returns the length of the underlying array
func (b crypoBytes) Size() int {
	return len(b)
}

// Returns a base64 representation of the array
func (b crypoBytes) Base64() string {
	return base64.StdEncoding.EncodeToString(b)
}

// Returns a base32 representation of the array
func (b crypoBytes) Base32() string {
	return base32.StdEncoding.EncodeToString(b)
}

func (b crypoBytes) Hex() string {
	return hex.Dump(b)
}

// Returns a string representation of the array
func (b crypoBytes) String() string {
	base64 := b.Base64()

	if b.Size() <= 32 {
		return base64
	} else {
		return fmt.Sprintf("%v... (total=%v)", base64[:32], len(base64))
	}
}

// Returns a new byte array for use as a cipher key
func (b crypoBytes) Hash(h Hash) (crypoBytes, error) {
	return h.Hash(b)
}

// Returns a new byte array for use as a cipher key
func (b crypoBytes) Pbkdf2(salt []byte, iter int, size int, h hash.Hash) crypoBytes {
	return pbkdf2.Key(b, salt, iter, size, func() hash.Hash {
		return h
	})
}

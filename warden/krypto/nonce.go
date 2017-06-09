package krypto

import (
	"io"

	"github.com/pkg/errors"
)

// Creates a new random nonce.  Nonces are essentially the same
// thing as initialization vectors and should be use
func GenNonce(rand io.Reader, size int) ([]byte, error) {
	return genRandomBytes(rand, size)
}

// Generates some random bytes (this should be considered )
func genRandomBytes(rand io.Reader, size int) ([]byte, error) {
	arr := make([]byte, size)
	if _, err := io.ReadFull(rand, arr); err != nil {
		return nil, errors.WithStack(err)
	}
	return arr, nil
}

package krypto

import (
	"io"

	"github.com/pkg/errors"
)

var (
	UnknownKeyAlgorithmError = errors.New("Warden:UnknownKeyAlgorithm")
)

type KeyAlgorithm int

const (
	Rsa KeyAlgorithm = iota
)

// Parses the private key from a standard binary format.
func (k KeyAlgorithm) Gen(rand io.Reader, strength int) (PrivateKey, error) {
	switch k {
	default:
		return nil, errors.WithStack(UnknownKeyAlgorithmError)
	case Rsa:
		return GenRsaKey(rand, strength)
	}
}

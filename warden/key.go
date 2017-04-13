package warden

import (
	"io"

	"github.com/pkg/errors"
)

var (
	UnknownKeyAlgorithmError = errors.New("Warden:UnknownKeyAlgorithm")
)

type KeyAlgorithm int

const (
	RSA KeyAlgorithm = iota
	// Elliptic
)

// Parses the public key from a standard binary format.
func (k KeyAlgorithm) ParsePublicKey(raw []byte) (PublicKey, error) {
	switch k {
	default:
		return nil, errors.WithStack(UnknownKeyAlgorithmError)
	case RSA:
		return parseRsaPublicKey(raw)
	}
}

// Parses the private key from a standard binary format.
func (k KeyAlgorithm) ParsePrivateKey(raw []byte) (PrivateKey, error) {
	switch k {
	default:
		return nil, errors.WithStack(UnknownKeyAlgorithmError)
	case RSA:
		return parseRsaPrivateKey(raw)
	}
}

// Parses the private key from a standard binary format.
func (k KeyAlgorithm) Gen(rand io.Reader, strength int) (PrivateKey, error) {
	switch k {
	default:
		return nil, errors.WithStack(UnknownKeyAlgorithmError)
	case RSA:
		return GenRsaKey(rand, strength)
	}
}

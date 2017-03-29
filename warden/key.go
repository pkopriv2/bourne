package warden

import "github.com/pkg/errors"

var (
	UnknownKeyAlgorithmError = errors.New("Warden:UnknownKeyAlgorithm")
)

type KeyAlgorithm int

const (
	RSA KeyAlgorithm = iota
	// DSA
	// ECDSA
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
	return nil, nil
}

type Signature interface {
	Hash() Hash
	Signature() []byte
}

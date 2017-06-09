package krypto

import (
	"encoding"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

// A universal wrapper over the gob/json encoders.  Messages can be encoded
// onto streams of these formats for free.
type Encoder interface {
	Encode(interface{}) ([]byte, error)
}

// A universal wrapper over the gob/json decoders.  Messages can be decoded
// onto streams of these formats for free.
type Decoder interface {
	Decode([]byte, interface{}) error
}

// A a signable object is one that has a consistent format for signing and verifying.
type Signable interface {
	SigningFormat() ([]byte, error)
}

// A signature is a cryptographically secure structure that may be used to both prove
// the authenticity of an accompanying document, as well as the identity of the signer.
type Signature struct {
	Key  string
	Hash Hash
	Data []byte
}

// Verifies the signature with the given public key.  Returns nil if the verification succeeded.
func (s Signature) Verify(key PublicKey, msg []byte) error {
	return key.Verify(s.Hash, msg, s.Data)
}

// Returns a simple string rep of the signature.
func (s Signature) String() string {
	return fmt.Sprintf("Signature(hash=%v): %v", s.Hash, Bytes(s.Data))
}

// A signer contains the knowledge necessary to digitally sign messages.
//
// For high-security environments, only a signer is required to authenticate with the
// system.  It is never necessary for the corresponding private key to leave the local
// system, which is convienent in enviroments where access to the private key is not
// possible (e.g. embedded hardware security modules).
type Signer interface {
	Public() PublicKey
	Sign(rand io.Reader, hash Hash, msg []byte) (Signature, error)
}

// Private keys represent proof of ownership of a public key and form the basis of
// how authentication, confidentiality and integrity concerns are managed within the
// ecosystem.  Warden goes to great lengths to ensure that private keys data are *NEVER*
// derivable by any untrusted actors (malicious or otherwise), including the system itself.
//
type PrivateKey interface {
	Signer
	Signable

	Algorithm() KeyAlgorithm
	Decrypt(rand io.Reader, hash Hash, ciphertext []byte) ([]byte, error)
	Destroy()
}

// Public keys are the basis of identity within the trust ecosystem.  In plain english,
// I don't trust your identity, I only trust your keys.  Therefore, risk planning starts
// with limiting the exposure of your trusted keys.  The more trusted a key, the greater
// the implications of a compromised one.
//
// The purpose of using Warden is not just to publish keys for your own personal use
// (although it may be used for that), it is ultimately expected to serve as a way that
// people can form groups of trust.
//
type PublicKey interface {
	Signable

	// Implementations must eventually have platform independent encodings
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler

	Id() string
	Algorithm() KeyAlgorithm
	Verify(hash Hash, msg []byte, sig []byte) error
	Encrypt(rand io.Reader, hash Hash, plaintext []byte) ([]byte, error)
}

// Signs the object with the signer and hash
func Sign(rand io.Reader, obj Signable, signer Signer, hash Hash) (Signature, error) {
	fmt, err := obj.SigningFormat()
	if err != nil {
		return Signature{}, errors.WithStack(err)
	}

	sig, err := signer.Sign(rand, hash, fmt)
	if err != nil {
		return Signature{}, errors.WithStack(err)
	}
	return sig, nil
}

// Verifies the object with the signer and hash
func Verify(obj Signable, key PublicKey, sig Signature) error {
	fmt, err := obj.SigningFormat()
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(key.Verify(sig.Hash, fmt, sig.Data))
}

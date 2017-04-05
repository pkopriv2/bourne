package warden

import (
	"bytes"
	"crypto/rsa"
	"encoding/gob"
	"io"

	"github.com/pkg/errors"
)

func GenRsaKey(rand io.Reader, bits int) (*rsaPrivateKey, error) {
	key, err := rsa.GenerateKey(rand, bits)
	if err != nil {
		return nil, errors.Wrapf(err, "Error generating private key [%v]", bits)
	}
	return &rsaPrivateKey{key}, nil
}

// Public RSA key implementation.
type rsaPublicKey struct {
	raw *rsa.PublicKey
}

func (r *rsaPublicKey) Algorithm() KeyAlgorithm {
	return RSA
}

func (r *rsaPublicKey) Id() string {
	hash, err := Bytes(r.Bytes()).Hash(SHA1)
	if err != nil {
		panic(err)
	}
	return hash.Base64()
}

func (r *rsaPublicKey) Verify(hash Hash, msg []byte, sig []byte) error {
	hashed, err := hash.Hash(msg)
	if err != nil {
		return errors.Wrapf(err, "Unable to hash message [%v] using alg [%v]", Bytes(msg), hash)
	}
	if err := rsa.VerifyPSS(r.raw, hash.Crypto(), hashed, sig, nil); err != nil {
		return errors.Wrapf(err, "Unable to verify signature [%v] with key [%v]", Bytes(sig), r.raw)
	}
	return nil
}

func (r *rsaPublicKey) Encrypt(rand io.Reader, hash Hash, msg []byte) ([]byte, error) {
	msg, err := rsa.EncryptOAEP(hash.Standard(), rand, r.raw, msg, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "Error encrypting message [%v] with key [%v]", Bytes(msg), r.raw)
	}
	return msg, nil
}

func (r *rsaPublicKey) Bytes() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(r.raw); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func parseRsaPublicKey(raw []byte) (*rsaPublicKey, error) {
	key := &rsa.PublicKey{}

	dec := gob.NewDecoder(bytes.NewBuffer(raw))
	if err := dec.Decode(&key); err != nil {
		return nil, errors.WithStack(err)
	}
	return &rsaPublicKey{key}, nil
}

// Private key implementation
type rsaPrivateKey struct {
	raw *rsa.PrivateKey
}

func (r *rsaPrivateKey) Algorithm() KeyAlgorithm {
	return RSA
}

func (r *rsaPrivateKey) public() *rsaPublicKey {
	return &rsaPublicKey{&r.raw.PublicKey}
}

func (r *rsaPrivateKey) Public() PublicKey {
	return r.public()
}

func (r *rsaPrivateKey) Sign(rand io.Reader, hash Hash, msg []byte) (Signature, error) {
	hashed, err := hash.Hash(msg)
	if err != nil {
		return Signature{}, errors.Wrapf(err, "Unable to hash message [%v] using alg [%v]", Bytes(msg), hash)
	}
	sig, err := rsa.SignPSS(rand, r.raw, hash.Crypto(), hashed, nil)
	if err != nil {
		return Signature{}, errors.Wrapf(err, "Unable to sign msg [%v]", Bytes(msg))
	}
	return Signature{hash, sig}, nil
}

func (r *rsaPrivateKey) Decrypt(rand io.Reader, hash Hash, ciphertext []byte) ([]byte, error) {
	plaintext, err := rsa.DecryptOAEP(hash.Standard(), rand, r.raw, ciphertext, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to decrypt ciphertext [%v]", Bytes(ciphertext))
	}
	return plaintext, nil
}

func (r *rsaPrivateKey) Destroy() {
	// FIXME: implement
}

func (r *rsaPrivateKey) Bytes() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(r.raw); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func parseRsaPrivateKey(raw []byte) (*rsaPrivateKey, error) {
	key := &rsa.PrivateKey{}

	dec := gob.NewDecoder(bytes.NewBuffer(raw))
	if err := dec.Decode(&key); err != nil {
		return nil, errors.WithStack(err)
	}
	return &rsaPrivateKey{key}, nil
}

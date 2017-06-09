package krypto

import (
	"bytes"
	"crypto/rsa"
	"encoding/gob"
	"io"

	"github.com/pkg/errors"
)

func GenRsaKey(rand io.Reader, bits int) (PrivateKey, error) {
	key, err := rsa.GenerateKey(rand, bits)
	if err != nil {
		return nil, errors.Wrapf(err, "Error generating private key [%v]", bits)
	}
	return &rsaPrivateKey{key}, nil
}

// Public RSA key implementation.
type rsaPublicKey struct {
	Raw *rsa.PublicKey
}

func (r *rsaPublicKey) Algorithm() KeyAlgorithm {
	return Rsa
}

func (r *rsaPublicKey) Id() string {
	fmt, err := r.SigningFormat()
	if err != nil {
		panic(err) // not supposed to happen!
	}

	hash, err := Bytes(fmt).Hash(SHA1)
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
	if err := rsa.VerifyPSS(r.Raw, hash.crypto(), hashed, sig, nil); err != nil {
		return errors.Wrapf(err, "Unable to verify signature [%v] with key [%v]", Bytes(sig), r.Id())
	}
	return nil
}

func (r *rsaPublicKey) Encrypt(rand io.Reader, hash Hash, msg []byte) ([]byte, error) {
	msg, err := rsa.EncryptOAEP(hash.standard(), rand, r.Raw, msg, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "Error encrypting message [%v] with key [%v]", Bytes(msg), r.Raw)
	}
	return msg, nil
}

func (r *rsaPublicKey) SigningFormat() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(r.Raw); err != nil {
		panic(err)
	}
	return buf.Bytes(), nil
}

func (r *rsaPublicKey) MarshalBinary() ([]byte, error) {
	ret, err := gobBytes(r.Raw)
	return ret, errors.WithStack(err)
}

func (r *rsaPublicKey) UnmarshalBinary(data []byte) error {
	r.Raw = &rsa.PublicKey{}
	_, err := parseGobBytes(data, &r.Raw)
	return errors.WithStack(err)
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
	Raw *rsa.PrivateKey
}

func (r *rsaPrivateKey) Algorithm() KeyAlgorithm {
	return Rsa
}

func (r *rsaPrivateKey) public() *rsaPublicKey {
	return &rsaPublicKey{&r.Raw.PublicKey}
}

func (r *rsaPrivateKey) Public() PublicKey {
	return r.public()
}

func (r *rsaPrivateKey) Sign(rand io.Reader, hash Hash, msg []byte) (Signature, error) {
	hashed, err := hash.Hash(msg)
	if err != nil {
		return Signature{}, errors.Wrapf(err, "Unable to hash message [%v] using alg [%v]", Bytes(msg), hash)
	}
	sig, err := rsa.SignPSS(rand, r.Raw, hash.crypto(), hashed, nil)
	if err != nil {
		return Signature{}, errors.Wrapf(err, "Unable to sign msg [%v]", Bytes(msg))
	}
	return Signature{r.public().Id(), hash, sig}, nil
}

func (r *rsaPrivateKey) Decrypt(rand io.Reader, hash Hash, ciphertext []byte) ([]byte, error) {
	plaintext, err := rsa.DecryptOAEP(hash.standard(), rand, r.Raw, ciphertext, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to decrypt ciphertext [%v]", Bytes(ciphertext))
	}
	return plaintext, nil
}

func (r *rsaPrivateKey) Destroy() {
	// FIXME: implement
}

func (r *rsaPrivateKey) SigningFormat() ([]byte, error) {
	return r.MarshalBinary()
}

func (r *rsaPrivateKey) MarshalBinary() ([]byte, error) {
	ret, err := gobBytes(r.Raw)
	return ret, errors.WithStack(err)
}

func (r *rsaPrivateKey) UnmarshalBinary(data []byte) error {
	r.Raw = &rsa.PrivateKey{}
	_, err := parseGobBytes(data, &r.Raw)
	return errors.WithStack(err)
}

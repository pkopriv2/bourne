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
	Raw *rsa.PublicKey
}

func (r *rsaPublicKey) Algorithm() KeyAlgorithm {
	return RSA
}

func (r *rsaPublicKey) Id() string {
	hash, err := cryptoBytes(r.format()).Hash(SHA1)
	if err != nil {
		panic(err)
	}
	return hash.Base64()
}

func (r *rsaPublicKey) Verify(hash Hash, msg []byte, sig []byte) error {
	hashed, err := hash.Hash(msg)
	if err != nil {
		return errors.Wrapf(err, "Unable to hash message [%v] using alg [%v]", cryptoBytes(msg), hash)
	}
	if err := rsa.VerifyPSS(r.Raw, hash.crypto(), hashed, sig, nil); err != nil {
		return errors.Wrapf(err, "Unable to verify signature [%v] with key [%v]", cryptoBytes(sig), r.Id())
	}
	return nil
}

func (r *rsaPublicKey) Encrypt(rand io.Reader, hash Hash, msg []byte) ([]byte, error) {
	msg, err := rsa.EncryptOAEP(hash.standard(), rand, r.Raw, msg, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "Error encrypting message [%v] with key [%v]", cryptoBytes(msg), r.Raw)
	}
	return msg, nil
}

func (r *rsaPublicKey) format() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(r.Raw); err != nil {
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
	Raw *rsa.PrivateKey
}

func (r *rsaPrivateKey) Algorithm() KeyAlgorithm {
	return RSA
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
		return Signature{}, errors.Wrapf(err, "Unable to hash message [%v] using alg [%v]", cryptoBytes(msg), hash)
	}
	sig, err := rsa.SignPSS(rand, r.Raw, hash.crypto(), hashed, nil)
	if err != nil {
		return Signature{}, errors.Wrapf(err, "Unable to sign msg [%v]", cryptoBytes(msg))
	}
	return Signature{r.public().Id(), hash, sig}, nil
}

func (r *rsaPrivateKey) Decrypt(rand io.Reader, hash Hash, ciphertext []byte) ([]byte, error) {
	plaintext, err := rsa.DecryptOAEP(hash.standard(), rand, r.Raw, ciphertext, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to decrypt ciphertext [%v]", cryptoBytes(ciphertext))
	}
	return plaintext, nil
}

func (r *rsaPrivateKey) Destroy() {
	// FIXME: implement
}

func (r *rsaPrivateKey) format() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(r.Raw); err != nil {
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

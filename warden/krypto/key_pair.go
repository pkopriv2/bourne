package krypto

import (
	"io"

	"github.com/pkg/errors"
)

// Basic options for generating a key/pair
type KeyPairOptions struct {
	Cipher Cipher
	Salt   SaltOptions
}

func buildKeyPairOptions(fns ...func(*KeyPairOptions)) KeyPairOptions {
	ret := KeyPairOptions{AES_256_GCM, buildSaltOptions()}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// Signed key pair
type SignedKeyPair struct {
	KeyPair
	Sig Signature
}

// An encrypted private key + plaintext public key
type KeyPair struct {
	Encoding string
	Pub      PublicKey
	Priv     SaltedCipherText
}

// Generates a new encrypted key pair
func NewKeyPair(rand io.Reader, enc Encoder, priv PrivateKey, pass []byte, fns ...func(*KeyPairOptions)) (KeyPair, error) {
	opts := buildKeyPairOptions(fns...)

	salt, err := GenSalt(rand, func(o *SaltOptions) {
		o.Proto(opts.Salt)
	})
	if err != nil {
		return KeyPair{}, errors.WithStack(err)
	}

	encoded, err := enc.Encode(priv)
	if err != nil {
		return KeyPair{}, errors.WithStack(err)
	}

	ciphertext, err := salt.Encrypt(rand, opts.Cipher, pass, encoded.Body)
	if err != nil {
		return KeyPair{}, errors.WithStack(err)
	}

	return KeyPair{encoded.Format, priv.Public(), ciphertext}, nil
}

func (p KeyPair) SigningFormat() ([]byte, error) {
	fmt, err := p.Pub.SigningFormat()
	return fmt, errors.WithStack(err)
}

func (p KeyPair) Sign(rand io.Reader, priv Signer, hash Hash) (SignedKeyPair, error) {
	sig, err := Sign(rand, p, priv, hash)
	if err != nil {
		return SignedKeyPair{}, errors.WithStack(err)
	}
	return SignedKeyPair{p, sig}, nil
}

func (p KeyPair) Decrypt(dec Decoder, key []byte) (ret PrivateKey, err error) {
	raw, err := p.Priv.Decrypt(key)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer raw.Destroy()

	ret, err = p.Pub.Algorithm().InitPriv()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	err = dec.Decode(raw.Encoding(p.Encoding), &ret)
	return
}

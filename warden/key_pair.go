package warden

import (
	"io"

	"github.com/pkg/errors"
)

// Basic options for generating a key/pair
type KeyPairOptions struct {
	Algorithm KeyAlgorithm
	Strength  int
	Cipher    SymmetricCipher
	Hash      Hash
	Iter      int
	Salt      int
}

func buildKeyPairOptions(fns ...func(*KeyPairOptions)) KeyPairOptions {
	ret := KeyPairOptions{Rsa, 2048, Aes256Gcm, SHA256, 1024, 32}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

type SignedKeyPair struct {
	KeyPair
	Sig Signature
}

// A key pair
type KeyPair struct {
	Pub  PublicKey
	Opts KeyPairOptions
	Priv CipherText
	Salt []byte
}

// Generates a new encrypted key pair
func encryptKey(rand io.Reader, creator Signer, priv PrivateKey, pass []byte, opts KeyPairOptions) (SignedKeyPair, error) {
	salt, err := genRandomBytes(rand, opts.Salt)
	if err != nil {
		return SignedKeyPair{}, errors.WithStack(err)
	}

	ciphertext, err := opts.Cipher.encrypt(
		rand, cryptoBytes(pass).Pbkdf2(salt, opts.Iter, opts.Cipher.KeySize(), opts.Hash.standard()), priv.format())
	if err != nil {
		return SignedKeyPair{}, errors.WithStack(err)
	}

	raw := KeyPair{priv.Public(), opts, ciphertext, salt}
	return raw.Sign(rand, creator, opts.Hash)
}

func (p KeyPair) SigningFormat() ([]byte, error) {
	return p.Pub.format(), nil
}

func (p KeyPair) Sign(rand io.Reader, priv Signer, hash Hash) (SignedKeyPair, error) {
	sig, err := sign(rand, p, priv, hash)
	if err != nil {
		return SignedKeyPair{}, errors.WithStack(err)
	}
	return SignedKeyPair{p, sig}, nil
}

func (p KeyPair) Decrypt(key []byte) (PrivateKey, error) {
	raw, err := p.Priv.Decrypt(
		cryptoBytes(key).Pbkdf2(
			p.Salt, p.Opts.Iter, p.Priv.Cipher.KeySize(), p.Opts.Hash.standard()))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer raw.Destroy()

	priv, err := p.Pub.Algorithm().parsePrivateKey(raw)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return priv, nil
}

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
	ret := KeyPairOptions{RSA, 2048, AES_256_GCM, SHA256, 1024, 32}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

type SignedKeyPair struct {
	KeyPair
	Sig Signature
}

// A signing key is an encrypted private key.  It may only be decrypted
// by someone who has been trusted to sign on its behalf.
type KeyPair struct {
	Pub  PublicKey
	Priv CipherText
	Opts KeyPairOptions
	Salt []byte
}

// Generates a new encrypted key pair
func genKeyPair(rand io.Reader, priv PrivateKey, pass []byte, opts KeyPairOptions) (KeyPair, error) {
	salt, err := generateRandomBytes(rand, opts.Salt)
	if err != nil {
		return KeyPair{}, errors.WithStack(err)
	}

	ciphertext, err := opts.Cipher.Encrypt(
		rand, Bytes(pass).Pbkdf2(salt, opts.Iter, opts.Cipher.KeySize(), opts.Hash.Standard()), priv.Bytes())
	if err != nil {
		return KeyPair{}, errors.WithStack(err)
	}

	return KeyPair{
		priv.Public(),
		ciphertext,
		opts,
		salt,
	}, nil
}

func (p KeyPair) Format() ([]byte, error) {
	return p.Pub.Bytes(), nil
}

func (p KeyPair) Sign(rand io.Reader, priv Signer, hash Hash) (SignedKeyPair, error) {
	fmt, err := p.Format()
	if err != nil {
		return SignedKeyPair{}, errors.WithStack(err)
	}

	sig, err := priv.Sign(rand, hash, fmt)
	if err != nil {
		return SignedKeyPair{}, errors.WithStack(err)
	}

	return SignedKeyPair{p, sig}, nil
}

func (p KeyPair) Decrypt(key []byte) (PrivateKey, error) {
	raw, err := p.Priv.Decrypt(
		Bytes(key).Pbkdf2(
			p.Salt, p.Opts.Iter, p.Priv.Cipher.KeySize(), p.Opts.Hash.Standard()))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer raw.Destroy()

	priv, err := p.Pub.Algorithm().ParsePrivateKey(raw)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return priv, nil
}

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
	ret := KeyPairOptions{RSA, 2048, Aes256Gcm, SHA256, 1024, 32}
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
	Opts KeyPairOptions
	priv cipherText
	salt []byte
}

// Generates a new encrypted key pair
func genKeyPair(rand io.Reader, priv PrivateKey, pass []byte, opts KeyPairOptions) (KeyPair, error) {
	salt, err := generateRandomBytes(rand, opts.Salt)
	if err != nil {
		return KeyPair{}, errors.WithStack(err)
	}

	ciphertext, err := opts.Cipher.encrypt(
		rand, cryptoBytes(pass).Pbkdf2(salt, opts.Iter, opts.Cipher.KeySize(), opts.Hash.standard()), priv.format())
	if err != nil {
		return KeyPair{}, errors.WithStack(err)
	}

	return KeyPair{
		priv.Public(),
		opts,
		ciphertext,
		salt,
	}, nil
}

func (p KeyPair) Format() ([]byte, error) {
	return p.Pub.format(), nil
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
	raw, err := p.priv.Decrypt(
		cryptoBytes(key).Pbkdf2(
			p.salt, p.Opts.Iter, p.priv.Cipher.KeySize(), p.Opts.Hash.standard()))
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

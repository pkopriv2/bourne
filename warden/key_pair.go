package warden

import (
	"io"

	"github.com/pkg/errors"
)

type SignedKeyPair struct {
	KeyPair
	Sig Signature
}

// A signing key is an encrypted private key.  It may only be decrypted
// by someone who has been trusted to sign on its behalf.
type KeyPair struct {
	Pub    PublicKey
	Priv   CipherText
	FnHash Hash
	FnSalt []byte
	FnIter int
}

// Generates a new encrypted key pair
func genKeyPair(rand io.Reader, priv PrivateKey, pass []byte, ciph SymmetricCipher, hsh Hash, saltSize int, iter int) (KeyPair, error) {
	salt, err := generateRandomBytes(rand, saltSize)
	if err != nil {
		return KeyPair{}, errors.WithStack(err)
	}

	ciphertext, err := ciph.Encrypt(
		rand, Bytes(pass).Pbkdf2(salt, iter, ciph.KeySize(), hsh.Standard()), priv.Bytes())
	if err != nil {
		return KeyPair{}, errors.WithStack(err)
	}

	return KeyPair{
		priv.Public(),
		ciphertext,
		hsh,
		salt,
		iter,
	}, nil
}

func (p KeyPair) Decrypt(key []byte) (PrivateKey, error) {
	raw, err := p.Priv.Decrypt(
		Bytes(key).Pbkdf2(
			p.FnSalt, p.FnIter, p.Priv.Cipher.KeySize(), p.FnHash.Standard()))
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

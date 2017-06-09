package krypto

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
)

type KeyExchangeOptions struct {
	Cipher Cipher
	Hash   Hash
}

func buildKeyExchangeOpts(fns ...func(*KeyExchangeOptions)) KeyExchangeOptions {
	ret := KeyExchangeOptions{AES_256_GCM, SHA256}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

type KeyExchange struct {
	KeyAlg    KeyAlgorithm
	KeyCipher Cipher
	KeyHash   Hash
	KeyBytes  Bytes
}

func generateKeyExchange(rand io.Reader, pub PublicKey, cipher Cipher, hash Hash) (KeyExchange, []byte, error) {
	rawCipherKey, err := initRandomSymmetricKey(rand, cipher)
	if err != nil {
		return KeyExchange{}, nil, errors.WithStack(err)
	}

	encCipherKey, err := pub.Encrypt(rand, hash, rawCipherKey)
	if err != nil {
		return KeyExchange{}, nil, errors.WithStack(err)
	}

	return KeyExchange{pub.Algorithm(), cipher, hash, encCipherKey}, rawCipherKey, nil
}

func (k KeyExchange) Decrypt(rand io.Reader, priv PrivateKey) ([]byte, error) {
	key, err := priv.Decrypt(rand, k.KeyHash, k.KeyBytes)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return key, nil
}

func (c KeyExchange) String() string {
	return fmt.Sprintf("AsymmetricCipherText(alg=%v,key=%v,val=%v)", c.KeyAlg, c.KeyHash, c.KeyBytes)
}

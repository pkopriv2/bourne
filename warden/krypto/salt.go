package krypto

import (
	"io"

	"github.com/pkg/errors"
)

// Options for generating a shared secret.
type SaltOptions struct {
	Hash Hash
	Iter int
	Size int
}

func defaultSaltOptions() SaltOptions {
	return SaltOptions{SHA256, 1024, 32}
}

func buildSaltOptions(fns ...func(*SaltOptions)) SaltOptions {
	ret := defaultSaltOptions()
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

type Salt struct {
	Hash  Hash
	Iter  int
	Nonce []byte
}

func GenSalt(rand io.Reader, fns ...func(*SaltOptions)) (Salt, error) {
	opts := buildSaltOptions(fns...)

	nonce, err := GenNonce(rand, opts.Size)
	if err != nil {
		return Salt{}, errors.Wrapf(err, "Error generating nonce of [%v] bytes", opts.Size)
	}

	return Salt{opts.Hash, opts.Iter, nonce}, nil
}

func (s Salt) Apply(val Bytes, size int) Bytes {
	return val.Pbkdf2(s.Nonce, s.Iter, size, s.Hash)
}

func (s Salt) Encrypt(rand io.Reader, cipher Cipher, key, msg Bytes) (SaltedCipherText, error) {
	ciphertext, err := cipher.Apply(rand, s.Apply(key, cipher.KeySize()), msg)
	if err != nil {
		return SaltedCipherText{}, errors.WithStack(err)
	}
	return SaltedCipherText{ciphertext, s}, nil
}

type SaltedCipherText struct {
	Msg  CipherText
	Salt Salt
}

func (s SaltedCipherText) Decrypt(key Bytes) (Bytes, error) {
	return s.Msg.Decrypt(s.Salt.Apply(key, s.Msg.Cipher.KeySize()))
}

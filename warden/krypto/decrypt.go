package krypto

import "github.com/pkg/errors"

// Decrypts the object with the key.
func Decrypt(obj Encrypted, key Signable) ([]byte, error) {
	fmt, err := key.SigningFormat()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ret, err := obj.Decrypt(fmt)
	return ret, errors.WithStack(err)
}

package warden

import "github.com/pkg/errors"

// A token is used to access a secure object.  A token may contain sensitive information
// that only the specific user should have access to and has been encrypted for use
// within THIS PROCESS SPACE.
type Token symCipherText

// Decrypts the token with the given key.
func (t Token) Decrypt(key []byte) ([]byte, error) {
	raw, err := symCipherText(t).Decrypt(key)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return raw, nil
}

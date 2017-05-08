package warden

import (
	"io"
	"time"
)

// The decrypted token.  Internal only and only decrypted by the owner of the token key.
type auth struct {
	IssuedTo string
	IssuedBy string
	Created  time.Time
	Expires  time.Time
}

func newToken(issuedTo string, issuedBy string, ttl time.Duration) auth {
	now := time.Now()
	return auth{issuedTo, issuedBy, time.Now(), now.Add(ttl)}
}

func (s auth) Sign(rand io.Reader, signer Signer, hash Hash) ([]byte, error) {
	return nil, nil
}

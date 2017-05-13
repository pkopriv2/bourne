package warden

import (
	"io"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// The decrypted token.  Internal only and only decrypted by the owner of the token key.
type signedAuth struct {
	auth
	Sig Signature
}

func signAuth(rand io.Reader, signer Signer, auth auth, hash Hash) (signedAuth, error) {
	sig, err := sign(rand, auth, signer, hash)
	if err != nil {
		return signedAuth{}, errors.WithStack(err)
	}
	return signedAuth{auth, sig}, nil
}

type authChallenge struct {
	Now time.Time
}

func newAuthChallenge() authChallenge {
	return authChallenge{time.Now()}
}

func (a authChallenge) Format() ([]byte, error) {
	fmt, err := gobBytes(a.Now)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return fmt, nil
}

type auth struct {
	IssuedTo uuid.UUID
	IssuedBy uuid.UUID
	Role     string
	Created  time.Time
	Expires  time.Time
}

func newAuth(issuedTo, issuedBy uuid.UUID, ttl time.Duration) auth {
	now := time.Now()
	return auth{issuedTo, issuedBy, "", time.Now(), now.Add(ttl)}
}

func (s auth) Format() ([]byte, error) {
	fmt, err := gobBytes(s)
	return fmt, errors.WithStack(err)
}

func (s auth) Sign(rand io.Reader, signer Signer, hash Hash) ([]byte, error) {
	return nil, nil
}

package warden

import (
	"io"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type Token struct {
	auth
	Sig Signature
}

func signAuth(rand io.Reader, signer Signer, auth auth, hash Hash) (Token, error) {
	sig, err := sign(rand, auth, signer, hash)
	if err != nil {
		return Token{}, errors.WithStack(err)
	}
	return Token{auth, sig}, nil
}

type signatureChallenge struct {
	Now time.Time
}

func newAuthChallenge() signatureChallenge {
	return signatureChallenge{time.Now()}
}

func (a signatureChallenge) Format() ([]byte, error) {
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

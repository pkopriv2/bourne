package warden

import (
	"io"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type SignedToken struct {
	Token
	Sig Signature
}

func (t SignedToken) Verify(key PublicKey) error {
	err := verify(t.Token, key, t.Sig)
	return errors.Wrapf(err, "Invalid token [%v, %v]", t.Created, t.Expires)
}

type Token struct {
	MemberId uuid.UUID
	Created  time.Time
	Expires  time.Time
}

func newToken(memberId uuid.UUID, ttl time.Duration) Token {
	now := time.Now()
	return Token{memberId, time.Now(), now.Add(ttl)}
}

func (s Token) Expired(now time.Time) bool {
	return s.Created.After(now) || s.Expires.Before(now)
}

func (s Token) SigningFormat() ([]byte, error) {
	fmt, err := gobBytes(s)
	return fmt, errors.WithStack(err)
}

func (s Token) Sign(rand io.Reader, signer Signer, hash Hash) (SignedToken, error) {
	sig, err := sign(rand, s, signer, hash)
	if err != nil {
		return SignedToken{}, errors.WithStack(err)
	}
	return SignedToken{s, sig}, nil
}

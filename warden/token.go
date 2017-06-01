package warden

import (
	"fmt"
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
	MemberId     uuid.UUID
	SubscriberId uuid.UUID
	Created      time.Time
	Expires      time.Time
	Claims       Signable
}

func newToken(memberId, subscriberId uuid.UUID, ttl time.Duration, args Signable) Token {
	now := time.Now()
	return Token{memberId, subscriberId, time.Now(), now.Add(ttl), args}
}

func (s Token) String() string {
	return fmt.Sprintf("Token(m=%v,s=%v): %v", formatUUID(s.MemberId), formatUUID(s.SubscriberId), s.Created)
}

func (s Token) Expired(now time.Time) bool {
	return s.Created.After(now) || s.Expires.Before(now)
}

func (s Token) SigningFormat() ([]byte, error) {
	var bodyFmt []byte
	var err error
	if s.Claims != nil {
		bodyFmt, err = s.Claims.SigningFormat()
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	fmt, err := gobBytes(tokenFmt{s.MemberId, s.SubscriberId, s.Created, s.Expires, bodyFmt})
	return fmt, errors.WithStack(err)
}

func (s Token) Sign(rand io.Reader, signer Signer, hash Hash) (SignedToken, error) {
	sig, err := sign(rand, s, signer, hash)
	if err != nil {
		return SignedToken{}, errors.WithStack(err)
	}
	return SignedToken{s, sig}, nil
}

type tokenFmt struct {
	MemberId     uuid.UUID
	SubscriberId uuid.UUID
	Created      time.Time
	Expires      time.Time
	Claims       []byte
}

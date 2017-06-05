package warden

import (
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
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
	Role      Role
	Created   time.Time
	Expires   time.Time
	Agreement MemberAgreement
	Claims    Signable
}

func newToken(agreement MemberAgreement, ttl time.Duration, role Role, args Signable) Token {
	now := time.Now()
	return Token{role, time.Now(), now.Add(ttl), agreement, args}
}

func (s Token) String() string {
	return fmt.Sprintf("Token(m=%v,s=%v): %v", formatUUID(s.Agreement.MemberId), formatUUID(s.Agreement.SubscriberId), s.Expires)
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

	fmt, err := gobBytes(tokenFmt{s.Agreement, s.Created, s.Expires, bodyFmt})
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
	Agreement MemberAgreement
	Created   time.Time
	Expires   time.Time
	Claims    []byte
}

package warden

import (
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

type Token struct {
	Auth
	Sig Signature
}

func (t Token) Verify(key PublicKey) error {
	err := verify(t.Auth, key, t.Sig)
	return errors.Wrapf(err, "Invalid token [%v, %v]", t.Created, t.Expires)
}

type sigChallenge struct {
	Now time.Time
}

func newSigChallenge(rand io.Reader, signer Signer, hash Hash) (sigChallenge, Signature, error) {
	now := sigChallenge{time.Now()}
	sig, err := now.Sign(rand, signer, hash)
	return now, sig, errors.WithStack(err)
}

func newSigLookup(key PublicKey) []byte {
	return stash.String("SIG:/").ChildString(key.Id())
}

func (a sigChallenge) Format() ([]byte, error) {
	fmt, err := gobBytes(a.Now)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return fmt, nil
}

func (s sigChallenge) Sign(rand io.Reader, signer Signer, hash Hash) (Signature, error) {
	sig, err := sign(rand, s, signer, hash)
	if err != nil {
		return Signature{}, errors.WithStack(err)
	}
	return sig, nil
}

type Auth struct {
	MemberId uuid.UUID
	Created  time.Time
	Expires  time.Time
}

func newAuth(memberId uuid.UUID, ttl time.Duration) Auth {
	now := time.Now()
	return Auth{memberId, time.Now(), now.Add(ttl)}
}

func (s Auth) Expired(now time.Time) bool {
	return s.Created.After(now) || s.Expires.Before(now)
}

func (s Auth) Format() ([]byte, error) {
	fmt, err := gobBytes(s)
	return fmt, errors.WithStack(err)
}

func (s Auth) Sign(rand io.Reader, signer Signer, hash Hash) (Token, error) {
	sig, err := sign(rand, s, signer, hash)
	if err != nil {
		return Token{}, errors.WithStack(err)
	}
	return Token{s, sig}, nil
}

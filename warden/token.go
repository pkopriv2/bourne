package warden

import (
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
)

// The decrypted token.  Internal only and only decrypted by the owner of the token key.
type token struct {
	IssuedTo string
	IssuedBy string
	Created  time.Time
	Expires  time.Time
}

func newToken(issuedTo string, issuedBy string, ttl time.Duration) token {
	now := time.Now()
	return token{issuedTo, issuedBy, time.Now(), now.Add(ttl)}
}

func (s token) Write(w scribe.Writer) {
	w.WriteString("issuedTo", s.IssuedTo)
	w.WriteString("issuedBy", s.IssuedBy)
	created, _ := s.Created.MarshalBinary()
	expires, _ := s.Created.MarshalBinary()
	w.WriteBytes("created", created)
	w.WriteBytes("expires", expires)
}

func (s token) Read(r scribe.Reader) (e error) {
	var created, expired []byte
	e = common.Or(e, r.ReadString("issuedTo", &s.IssuedTo))
	e = common.Or(e, r.ReadString("issuedBy", &s.IssuedBy))
	e = common.Or(e, r.ReadBytes("created", &created))
	e = common.Or(e, r.ReadBytes("expired", &expired))
	e = common.Or(e, s.Created.UnmarshalBinary(created))
	e = common.Or(e, s.Expires.UnmarshalBinary(created))
	return
}

func (s token) Sign(rand io.Reader, signer Signer, hash Hash) ([]byte, error) {
	return nil, nil
}

func (s token) Bytes() []byte {
	return scribe.Write(s).Bytes()
}

func (s token) Parse(raw []byte) error {
	msg, err := scribe.Parse(raw)
	if err != nil {
		return errors.WithStack(err)
	}

	return s.Read(msg)
}

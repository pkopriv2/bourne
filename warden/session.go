package warden

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// The decrypted session.  Internal only and only decrypted by the owner of the session key.
type session struct {
	IssuedTo uuid.UUID
	IssuedBy uuid.UUID
	Created  time.Time
	Expires  time.Time
}

func parseSession(raw []byte) (s session, e error) {
	e = s.Parse(raw)
	return
}

func newSession(issuedTo uuid.UUID, issuedBy uuid.UUID, ttl time.Duration) session {
	now := time.Now()
	return session{issuedTo, issuedBy, time.Now(), now.Add(ttl)}
}

func (s session) Write(w scribe.Writer) {
	w.WriteUUID("issuedTo", s.IssuedTo)
	w.WriteUUID("issuedBy", s.IssuedBy)
	created, _ := s.Created.MarshalBinary()
	expires, _ := s.Created.MarshalBinary()
	w.WriteBytes("created", created)
	w.WriteBytes("expires", expires)
}

func (s session) Read(r scribe.Reader) (e error) {
	var created, expired []byte
	e = common.Or(e, r.ReadUUID("issuedTo", &s.IssuedTo))
	e = common.Or(e, r.ReadUUID("issuedBy", &s.IssuedBy))
	e = common.Or(e, r.ReadBytes("created", &created))
	e = common.Or(e, r.ReadBytes("expired", &expired))
	e = common.Or(e, s.Created.UnmarshalBinary(created))
	e = common.Or(e, s.Expires.UnmarshalBinary(created))
	return
}

func (s session) Bytes() []byte {
	return scribe.Write(s).Bytes()
}

func (s session) Parse(raw []byte) error {
	msg, err := scribe.Parse(raw)
	if err != nil {
		return errors.WithStack(err)
	}

	return s.Read(msg)
}

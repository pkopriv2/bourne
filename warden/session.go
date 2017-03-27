package warden

import (
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// A session encodes an authenticated session.  Sessions are specific to
// the process in which it was created and cannot be shared.
type Session symCipherText

// Decryptes the session text into a useable session.
func (s Session) Decrypt(key []byte) (session, error) {
	raw, err := symCipherText(s).Decrypt(key)
	if err != nil {
		return session{}, errors.WithStack(err)
	}

	ret, err := parseSession(raw)
	if err != nil {
		return session{}, errors.WithStack(err)
	}

	return ret, err
}

// Issues a token that embeds the given encrypted message using the cipher and key.
func (s Session) IssueToken(rand io.Reader, cipher SymCipher, key []byte, meta []byte) (Token, error) {
	enc, err := symmetricEncrypt(rand, cipher, key, meta)
	if err != nil {
		return Token{}, errors.WithStack(err)
	}

	return Token(enc), nil
}

// The decrypted session.  Internal only and only decryptable by the owner of the session key.
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

// Issues a token that embeds the given encrypted message using the cipher and key.
func (s session) Issue(rand io.Reader, cipher SymCipher, key []byte) (Session, error) {
	enc, err := symmetricEncrypt(rand, cipher, key, s.Bytes())
	if err != nil {
		return Session{}, errors.WithStack(err)
	}

	return Session(enc), nil
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
	var created []byte
	var expired []byte
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

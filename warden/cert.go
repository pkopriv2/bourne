package warden

import (
	"bufio"
	"bytes"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// Trust levels dictate the terms for what actions a user can take on a domain.
type LevelOfTrust int

const (
	Verify LevelOfTrust = iota + 10
	Encryption
	Sign
	Invite
	Revoke
	Publish
	Destroy
)

func (l LevelOfTrust) Greater(o LevelOfTrust) bool {
	return l > o
}

func (l LevelOfTrust) EnsureExceeded(o LevelOfTrust) error {
	if l > o {
		return newLevelOfTrustError(l, o)
	}
	return nil
}

func newLevelOfTrustError(expected LevelOfTrust, actual LevelOfTrust) error {
	return errors.Wrapf(TrustError, "Expected level of trust [%v] got [%v]", expected, actual)
}

// A certificate is a receipt that trust has been established.
type Certificate struct {
	Fmt       int
	Id        uuid.UUID
	Domain    string
	Issuer    string
	Trustee   string
	Level     LevelOfTrust
	IssuedAt  time.Time
	ExpiresAt time.Time
}

func newCertificate(domain string, issuer string, trustee string, lvl LevelOfTrust, ttl time.Duration) Certificate {
	now := time.Now()
	return Certificate{0, uuid.NewV1(), domain, issuer, trustee, lvl, now, now.Add(ttl)}
}

// Verifies that the signature matches the certificate contents.
func (c Certificate) Verify(key PublicKey, sig Signature) error {
	return sig.Verify(key, c.Bytes())
}

// Signs the certificate with the private key.
func (c Certificate) Sign(rand io.Reader, key PrivateKey, hash Hash) (Signature, error) {
	sig, err := key.Sign(rand, hash, c.Bytes())
	if err != nil {
		return Signature{}, errors.Wrapf(err, "Error signing certificate [%v]", c)

	}
	return sig, nil
}

// Returns a consistent byte representation of a certificate
func (c Certificate) Bytes() []byte {
	buf := &bytes.Buffer{}
	w := scribe.NewStreamWriter(bufio.NewWriter(buf))
	c.Stream(w)
	w.Flush()
	return buf.Bytes()
}

// FIXME: Streams the

// Returns a consistent byte representation of a certificate
func (c Certificate) Stream(w scribe.StreamWriter) {
	w.PutUUID(c.Id)
	w.PutString(c.Domain)
	w.PutString(c.Issuer)
	w.PutString(c.Trustee)
}

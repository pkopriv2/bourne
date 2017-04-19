package warden

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// Trust levels dictate the terms for what actions a user can take on a domain.
type LevelOfTrust int

const (
	Verify LevelOfTrust = iota + 10
	Encrypt
	Sign
	Invite
	Revoke
	Publish
	Destroy
	Creator
)

// FIXME: Horrible...horrible
func (l LevelOfTrust) Greater(o LevelOfTrust) bool {
	return l > o
}

// FIXME: Horrible...horrible...horribler
func (l LevelOfTrust) Verify(o LevelOfTrust) error {
	if l > o {
		return newLevelOfTrustError(l, o)
	}
	return nil
}

func (l LevelOfTrust) String() string {
	switch l {
	default:
		return "Unknown"
	case Verify:
		return "Verify"
	case Encrypt:
		return "Encryption"
	case Sign:
		return "Sign"
	case Invite:
		return "Invite"
	case Revoke:
		return "Revoke"
	case Publish:
		return "Publish"
	case Destroy:
		return "Destroy"
	case Creator:
		return "Creator"
	}
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

// Returns the ttl of the certificate ()
func (c Certificate) Duration() time.Duration {
	return c.ExpiresAt.Sub(c.IssuedAt)
}

// Verifies that the signature matches the certificate contents.
func (c Certificate) Verify(key PublicKey, sig Signature) error {
	return sig.Verify(key, c.Format())
}

// Signs the certificate with the private key.
func (c Certificate) Sign(rand io.Reader, key PrivateKey, hash Hash) (Signature, error) {
	sig, err := key.Sign(rand, hash, c.Format())
	if err != nil {
		return Signature{}, errors.Wrapf(err, "Error signing certificate [%v]", c)
	}
	return sig, nil
}

// Returns a consistent byte representation of a certificate
func (c Certificate) Format() []byte {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(&c)
	return buf.Bytes()
}

// Returns a consistent string representation of a certificate
func (c Certificate) String() string {
	return fmt.Sprintf("Cert(domain=%v,issuer=%v,trustee=%v,lvl=%v): %v",
		c.Domain, c.Issuer, c.Trustee, c.Level, c.ExpiresAt.Sub(c.IssuedAt))
}

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

// FIXME: Horrible...horrible...horribler
func (l LevelOfTrust) verify(o LevelOfTrust) error {
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

type SignedCertificate struct {
	Certificate

	TrustSig   Signature
	IssuerSig  Signature
	TrusteeSig Signature
}

func signCertificate(rand io.Reader, c Certificate, trust, issuer, trustee Signer, h Hash) (SignedCertificate, error) {
	trustSig, err := sign(rand, c, trust, h)
	if err != nil {
		return SignedCertificate{}, errors.WithStack(err)
	}

	issuerSig, err := sign(rand, c, issuer, h)
	if err != nil {
		return SignedCertificate{}, errors.WithStack(err)
	}

	trusteeSig, err := sign(rand, c, issuer, h)
	if err != nil {
		return SignedCertificate{}, errors.WithStack(err)
	}

	return SignedCertificate{c, trustSig, issuerSig, trusteeSig}, nil
}

func (s SignedCertificate) Verify(trust, issuer, trustee PublicKey) error {
	if err := verify(s.Certificate, trust, s.TrustSig); err != nil {
		return errors.WithStack(err)
	}
	if err := verify(s.Certificate, issuer, s.IssuerSig); err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(verify(s.Certificate, trustee, s.TrusteeSig))
}

// A certificate is a receipt that trust has been established.
type Certificate struct {
	Fmt       int
	Id        uuid.UUID
	Trust     uuid.UUID
	Issuer    uuid.UUID
	Trustee   uuid.UUID
	Level     LevelOfTrust
	IssuedAt  time.Time
	ExpiresAt time.Time
}

func newCertificate(domain, issuer, trustee uuid.UUID, lvl LevelOfTrust, ttl time.Duration) Certificate {
	now := time.Now()
	return Certificate{0, uuid.NewV1(), domain, issuer, trustee, lvl, now, now.Add(ttl)}
}

// Returns the ttl of the certificate ()
func (c Certificate) Duration() time.Duration {
	return c.ExpiresAt.Sub(c.IssuedAt)
}

// Verifies that the signature matches the certificate contents.
func (c Certificate) Verify(key PublicKey, sig Signature) error {
	return verify(c, key, sig)
}

// Returns a consistent byte representation of a certificate.  Used for signing.
func (c Certificate) Format() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Returns a consistent string representation of a certificate
func (c Certificate) String() string {
	return fmt.Sprintf("Cert(domain=%v,issuer=%v,trustee=%v,lvl=%v): %v",
		c.Trust, c.Issuer, c.Trustee, c.Level, c.ExpiresAt.Sub(c.IssuedAt))
}

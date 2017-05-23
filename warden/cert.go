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

// The level of trust establishes the terms by which different actors
// have agreed to act with respect to a shared resource.  Enforcing this
// becomes the job of a third party.
//
// To affirm that consumers have indeed established a relationship, a
// trusted third party is expected to enforce the distribution of the
// shared resource based on the terms of their agreement.
type LevelOfTrust int

const (
	None LevelOfTrust = iota

	// A beneficiary is someone who receives the benefits of a trust.
	// In more realistic terms, a beneficiary can view the data within
	// a trust but cannot update it.
	Beneficiary

	// A manager has been entrusted to act on behalf of the trust - which
	// means that a manager can both view and update the repository data.
	// However, managers cannot invite others to manage in the trust.
	Manager

	// A director is a manager that may also appoint and remove any members
	// at will.
	Director

	// The grantor is the owner of the trust.  The grantor can appoint
	// or remove members at will and is the only member than may destroy
	// a trust.  Grantors may also choose to transfer their grantor status
	// to other members.
	Grantor
)

func (l LevelOfTrust) MetBy(o LevelOfTrust) bool {
	return o >= l
}

func (l LevelOfTrust) String() string {
	switch l {
	default:
		return "Unknown"
	case Beneficiary:
		return "Beneficiary"
	case Manager:
		return "Manager"
	case Director:
		return "Director"
	case Grantor:
		return "Grantor"
	}
}

func newLevelOfTrustError(expected LevelOfTrust, actual LevelOfTrust) error {
	return errors.Wrapf(TrustError, "Expected level of trust [%v] got [%v]", expected, actual)
}

// A SignedCertificate is a proof that a specific level of trust has been
// established by one party (the trustee), acting on behalf of a shared
// resource (i.e. the trust).  A valid certificate proves the following
// assertions:
//
//  * The issuer did indeed create the certificate.
//  * The issuer was in possession of the trust signing key.
//  * The trustee accepted the terms of the certificate.
//
// The creation of a signed certificate happens in three stages:
//  1. The issuance of the certificate.
//  2. The signature of the certificate by the trustee.
//  3. the registration of the certificate with the 3rd-party
//
type SignedCertificate struct {
	Certificate

	// The issuer of the certificate.
	IssuerSignature Signature

	// The shared signing key signature of the certificate.
	TrustSignature Signature

	// The recipient of the certificate.
	TrusteeSignature Signature
}

// Signs the certificate.
func signCertificate(rand io.Reader, c Certificate, trust, issuer, trustee Signer, h Hash) (SignedCertificate, error) {
	issuerSig, err := sign(rand, c, issuer, h)
	if err != nil {
		return SignedCertificate{}, errors.WithStack(err)
	}

	trustSig, err := sign(rand, c, trust, h)
	if err != nil {
		return SignedCertificate{}, errors.WithStack(err)
	}

	trusteeSig, err := sign(rand, c, issuer, h)
	if err != nil {
		return SignedCertificate{}, errors.WithStack(err)
	}

	return SignedCertificate{c, issuerSig, trustSig, trusteeSig}, nil
}

func (s SignedCertificate) Verify(issuer, trust, trustee PublicKey) error {
	if err := verify(s.Certificate, issuer, s.IssuerSignature); err != nil {
		return errors.WithStack(err)
	}
	if err := verify(s.Certificate, trust, s.TrustSignature); err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(verify(s.Certificate, trustee, s.TrusteeSignature))
}

// A certificate is a receipt that trust has been established.
type Certificate struct {
	Fmt       int
	Id        uuid.UUID
	TrustId   uuid.UUID
	IssuerId  uuid.UUID
	TrusteeId uuid.UUID
	Level     LevelOfTrust
	IssuedAt  time.Time
	ExpiresAt time.Time
}

func newCertificate(domain, issuer, trustee uuid.UUID, lvl LevelOfTrust, ttl time.Duration) Certificate {
	now := time.Now()
	return Certificate{0, uuid.NewV1(), domain, issuer, trustee, lvl, now, now.Add(ttl)}
}

// Returns a flag indicating whether the cert has been expired.
func (c Certificate) Expired(now time.Time, skew time.Duration) bool {
	return c.IssuedAt.After(now) || c.ExpiresAt.Before(now)
}

// Returns a boolean indicating whether the level of trust within the certificate
// meets or exceeds the given level of trust.
func (c Certificate) Meets(lvl LevelOfTrust) bool {
	return lvl.MetBy(c.Level)
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
	return fmt.Sprintf("Cert(trust=%v,issuer=%v,trustee=%v,lvl=%v): %v",
		c.TrustId, c.IssuerId, c.TrusteeId, c.Level, c.ExpiresAt.Sub(c.IssuedAt))
}

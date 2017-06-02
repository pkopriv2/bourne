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
func (c Certificate) SigningFormat() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Returns a consistent string representation of a certificate
func (c Certificate) String() string {
	return fmt.Sprintf("Cert(id=%v,trustId=%v,issuerId=%v,trusteeId=%v,lvl=%v): %v",
		formatUUID(c.Id), formatUUID(c.TrustId), formatUUID(c.IssuerId), formatUUID(c.TrusteeId), c.Level, c.ExpiresAt.Sub(c.IssuedAt))
}

func formatUUID(id uuid.UUID) string {
	return fmt.Sprintf("%v", id.String()[:8])
}

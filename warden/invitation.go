package warden

import (
	"time"

	uuid "github.com/satori/go.uuid"
)

type InvitationOptions struct {
}

// An invitation is a cryptographically secured message asking the recipient to share in the
// management of a domain. The invitation may only be accepted by the intended recipient.
// These technically can be shared publicly, but exposure should be limited (typically only the
// trust system needs to know).
type Invitation struct {
	Id uuid.UUID

	Domain  string
	Issuer  string
	Trustee string

	Level LevelOfTrust

	IssuedAt  time.Time
	StartsAt  time.Time
	ExpiresAt time.Time

	key KeyExchange
	msg CipherText

	// Not part of signed contents.
	DomainSignature Signature
	IssuerSignature Signature
}

// ifunc generateInvitation(rand io.Reader, oracle) (, error) {
// return Invitation{}, nil
// }
//
func (i Invitation) Bytes() []byte {
	return nil
}

// Verifies that the signature matches the certificate contents.
func (c Invitation) Verify(key PublicKey, signature Signature) error {
	return nil
}

// Signs the certificate with the private key.
func (c Invitation) Sign(key PrivateKey, hash Hash) (Signature, error) {
	return Signature{}, nil
}

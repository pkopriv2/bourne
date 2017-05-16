package warden

import (
	"time"

	uuid "github.com/satori/go.uuid"
)

type Transport interface {

	// Registers a new subscriber.
	Register(cancel <-chan struct{}, subscriber Membership, auth AccessShard) error

	// Loads public key by subscriber
	TokenBySignature(cancel <-chan struct{}, lookup []byte, challenge sigChallenge, sig Signature, ttl time.Duration) (Token, error)

	// // Returns the subscriber of the given key.
	MemberByLookup(cancel <-chan struct{}, t Token, lookup []byte) (Member, AccessCode, bool, error)

	// Loads invitations by subscriber and dom
	InvitationById(cancel <-chan struct{}, t Token, id uuid.UUID) (Invitation, bool, error)

	// Loads invitations by subscriber
	InvitationsBySubscriber(cancel <-chan struct{}, t Token, id uuid.UUID, opts PagingOptions) ([]Invitation, error)

	// Registers an invitation with the trust service.
	InvitationRegister(cancel <-chan struct{}, t Token, i Invitation) error

	// Revokes a certificate.
	InvitationRevoke(cancel <-chan struct{}, t Token, id uuid.UUID) error

	// Loads active certificates by subscriber
	CertsBySubscriber(cancel <-chan struct{}, t Token, id uuid.UUID, opts PagingOptions) ([]Certificate, error)

	// Loads active certs by domain.
	CertsByTrust(cancel <-chan struct{}, t Token, id uuid.UUID, opt PagingOptions) ([]Certificate, error)

	// Registers a certificate (and corresponding oracle key).
	CertRegister(cancel <-chan struct{}, t Token, c SignedCertificate, k SignedEncryptedShard) error

	// Revokes a certificate.
	CertRevoke(cancel <-chan struct{}, t Token, id uuid.UUID) error

	// Loads the domain by id
	TrustById(cancel <-chan struct{}, t Token, id uuid.UUID) (Trust, bool, error)

	// Loads the trust by subscriber
	TrustsBySubscriber(cancel <-chan struct{}, t Token, id uuid.UUID, beg, end int) ([]Trust, error)

	// Registers a newly created domain.
	RegisterTrust(cancel <-chan struct{}, t Token, trust Trust) error
}

package warden

import (
	"io"
	"time"

	uuid "github.com/satori/go.uuid"
)

type Transport interface {
	io.Closer

	// Registers a new subscriber.
	MemberRegister(cancel <-chan struct{}, t SignedToken, core memberCore, shard memberShard, acct []byte, auth []byte, tokenTTL time.Duration) (SignedToken, error)

	// // Registers a new subscriber.
	MemberAuthRegister(cancel <-chan struct{}, t SignedToken, memberId uuid.UUID, shard memberShard, auth []byte) error

	// Authenticates using
	Authenticate(cancel <-chan struct{}, acctLookup, authId, authArgs []byte, ttl time.Duration) (SignedToken, error)

	// Returns the mmeber with the given id.  (only if authorized by token owner's membership)
	MemberByIdAndAuth(cancel <-chan struct{}, t SignedToken, id uuid.UUID, authId []byte) (memberCore, memberShard, bool, error)

	// Returns the signing key of the given member
	MemberSigningKeyById(cancel <-chan struct{}, t SignedToken, id uuid.UUID) (PublicKey, bool, error)

	// Returns the invite key of the given member
	MemberInviteKeyById(cancel <-chan struct{}, t SignedToken, id uuid.UUID) (PublicKey, bool, error)

	// Loads invitations by subscriber and dom
	InvitationById(cancel <-chan struct{}, t SignedToken, id uuid.UUID) (Invitation, bool, error)

	// Loads invitations by subscriber
	InvitationsByMember(cancel <-chan struct{}, t SignedToken, id uuid.UUID, opts PagingOptions) ([]Invitation, error)

	// Loads invitations by subscriber
	InvitationsByTrust(cancel <-chan struct{}, t SignedToken, id uuid.UUID, opts PagingOptions) ([]Invitation, error)

	// Registers an invitation with the trust service.
	InvitationRegister(cancel <-chan struct{}, t SignedToken, i Invitation) error

	// Revokes a certificate.
	InvitationRevoke(cancel <-chan struct{}, t SignedToken, id uuid.UUID) error

	// Loads active certificates by subscriber
	CertsByMember(cancel <-chan struct{}, t SignedToken, id uuid.UUID, opts PagingOptions) ([]SignedCertificate, error)

	// Loads active certs by domain.
	CertsByTrust(cancel <-chan struct{}, t SignedToken, id uuid.UUID, opt PagingOptions) ([]SignedCertificate, error)

	// Registers a certificate (and corresponding oracle key).
	CertRegister(cancel <-chan struct{}, t SignedToken, c SignedCertificate, k trustCode) error

	// Revokes a certificate.
	CertRevoke(cancel <-chan struct{}, t SignedToken, trusteeId, trustId uuid.UUID) error

	// Loads the domain by id
	TrustById(cancel <-chan struct{}, t SignedToken, id uuid.UUID) (Trust, bool, error)

	// Loads the trust by subscriber
	TrustsByMember(cancel <-chan struct{}, t SignedToken, id uuid.UUID, opts PagingOptions) ([]Trust, error)

	// Registers a newly created domain.
	TrustRegister(cancel <-chan struct{}, t SignedToken, trust Trust) error
}

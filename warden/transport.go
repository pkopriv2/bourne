package warden

import uuid "github.com/satori/go.uuid"

type tokenizer func(cancel <-chan struct{}) (Token, error)

type transport interface {

	// Registers a new subscriber.
	Register(cancel <-chan struct{}, subscriber Subscriber, auth AccessShard) error

	// Loads public key by subscriber
	AuthBySignature(cancel <-chan struct{}, key string, challenge signatureChallenge, sig Signature) (Token, error)

	// Returns the subscriber of the given key.
	SubscriberByKey(cancel <-chan struct{}, auth tokenizer, key string) (Subscriber, bool, error)

	// Loads invitations by subscriber and dom
	InvitationById(cancel <-chan struct{}, a tokenizer, id uuid.UUID) (Invitation, bool, error)

	// Loads invitations by subscriber
	InvitationsBySubscriber(cancel <-chan struct{}, a tokenizer, id uuid.UUID, opts PagingOptions) ([]Invitation, error)

	// Registers an invitation with the trust service.
	InvitationRegister(cancel <-chan struct{}, a tokenizer, i Invitation) error

	// Revokes a certificate.
	InvitationRevoke(cancel <-chan struct{}, a tokenizer, id uuid.UUID) error

	// Loads active certificates by subscriber
	CertsBySubscriber(cancel <-chan struct{}, a tokenizer, id uuid.UUID, opts PagingOptions) ([]Certificate, error)

	// Loads active certs by domain.
	CertsByTrust(cancel <-chan struct{}, a tokenizer, id uuid.UUID, opt PagingOptions) ([]Certificate, error)

	// Registers a certificate (and corresponding oracle key).
	CertRegister(cancel <-chan struct{}, a tokenizer, c SignedCertificate, k SignedEncryptedShard) error

	// Revokes a certificate.
	CertRevoke(cancel <-chan struct{}, a tokenizer, id uuid.UUID) error

	// Loads the domain by id
	TrustById(cancel <-chan struct{}, a tokenizer, id uuid.UUID) (Trust, bool, error)

	// Loads the trust by subscriber
	TrustsBySubscriber(cancel <-chan struct{}, a tokenizer, id uuid.UUID, beg, end int) ([]Trust, error)

	// Registers a newly created domain.
	RegisterTrust(cancel <-chan struct{}, a tokenizer, dom Trust) error
}

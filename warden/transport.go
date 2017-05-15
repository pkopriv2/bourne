package warden

import uuid "github.com/satori/go.uuid"

type tokenizer func(cancel <-chan struct{}) (Token, error)

type transport struct {
	Auth        authTransport
	// Keys        keyTransport
	Certs       certTransport
	Invites     inviteTransport
	Trusts      trustTransport
	Subscribers subTransport
}

type subTransport interface {

	// Registers a subscriber with a signature challenge.
	RegisterBySignature(cancel <-chan struct{}, subscriber Subscriber, challenge signatureChallenge, sig Signature) error

	SubscriberByKey(cancel <-chan struct{}, auth tokenizer, key string) (Subscriber, bool, error)
}

type authTransport interface {

	// Loads public key by subscriber
	AuthBySignature(cancel <-chan struct{}, key string, challenge signatureChallenge, sig Signature) (Token, error)
}

type inviteTransport interface {

	// Loads invitations by subscriber and dom
	InvitationById(cancel <-chan struct{}, a tokenizer, id uuid.UUID) (Invitation, bool, error)

	// Loads invitations by subscriber
	InvitationsBySubscriber(cancel <-chan struct{}, a tokenizer, id uuid.UUID, opts PagingOptions) ([]Invitation, error)

	// Loads invitations by dom
	// InvitationsByTrust(cancel <-chan struct{}, a tokenizer, id uuid.UUID, opts PagingOptions) ([]Invitation, error)


	// Registers an invitation with the trust service.
	Upload(cancel <-chan struct{}, a tokenizer, i Invitation) error

	// Revokes a certificate.
	Revoke(cancel <-chan struct{}, a tokenizer, id uuid.UUID) error
}

type certTransport interface {

	// Loads active certificates by subscriber
	ActiveBySubscriber(cancel <-chan struct{}, a tokenizer, id uuid.UUID, opts PagingOptions) ([]Certificate, error)

	// Loads active certs by domain.
	ActiveByTrust(cancel <-chan struct{}, a tokenizer, id uuid.UUID, opt PagingOptions) ([]Certificate, error)

	// Registers a certificate (and corresponding oracle key).
	Register(cancel <-chan struct{}, a tokenizer, c SignedCertificate, k signedEncryptedShard) error

	// Revokes a certificate.
	Revoke(cancel <-chan struct{}, a tokenizer, id uuid.UUID) error
}

type trustTransport interface {

	// Loads the domain by id
	ById(cancel <-chan struct{}, a tokenizer, id uuid.UUID) (Trust, bool, error)

	// Loads the trust by subscriber
	BySubscriber(cancel <-chan struct{}, a tokenizer, id uuid.UUID, beg, end int) ([]Trust, error)

	// Registers a newly created domain.
	Register(cancel <-chan struct{}, a tokenizer, dom Trust, sign SignedKeyPair) error
}

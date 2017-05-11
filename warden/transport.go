package warden

import uuid "github.com/satori/go.uuid"

type tokenizer func(cancel <-chan struct{}) (signedAuth, error)

type transport struct {
	Auth    authTransport
	Keys    keyTransport
	Certs   certTransport
	Invites inviteTransport
	Trusts  trustTransport
}

type subTransport interface {
	ById(cancel <-chan struct{}, id uuid.UUID) (Subscriber, error)
}

type authTransport interface {

	// Loads public key by subscriber
	BySignature(cancel <-chan struct{}, key string, msg []byte, sig Signature) (signedAuth, error)
}

type keyTransport interface {

	// Loads public key by subscriber
	ByTrustAndType(cancel <-chan struct{}, a tokenizer, id uuid.UUID, alias KeyType) (KeyPair, error)

	// Loads public key by subscriber
	BySubscriberAndType(cancel <-chan struct{}, a tokenizer, id uuid.UUID, alias KeyType) (KeyPair, error)

	// Loads public key by subscriber
	ById(cancel <-chan struct{}, a tokenizer, id uuid.UUID, alias KeyType) (KeyPair, error)

	// Loads public key by subscriber
	PublicBySubscriber(cancel <-chan struct{}, a tokenizer, id uuid.UUID) (PublicKey, error)

	// Loads the public key by dom
	PublicByTrust(cancel <-chan struct{}, a tokenizer, id uuid.UUID) (PublicKey, error)
}

type inviteTransport interface {

	// Loads invitations by subscriber and dom
	ById(cancel <-chan struct{}, a tokenizer, id uuid.UUID) (Invitation, bool, error)

	// Loads invitations by subscriber
	BySubscriber(cancel <-chan struct{}, a tokenizer, id uuid.UUID, opts PagingOptions) ([]Invitation, error)

	// Loads invitations by dom
	ByTrust(cancel <-chan struct{}, a tokenizer, id uuid.UUID, opts PagingOptions) ([]Invitation, error)

	// Loads invitations by subscriber and dom
	BySubscriberAndTrust(cancel <-chan struct{}, a tokenizer, subscriber, dom string) (Invitation, bool, error)

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

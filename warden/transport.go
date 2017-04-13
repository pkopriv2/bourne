package warden

import uuid "github.com/satori/go.uuid"

type Net struct {
	Keys    KeyTransport
	Certs   CertTransport
	Invites InvitationTransport
	Domains DomainTransport
}

type KeyTransport interface {

	// Loads public key by subscriber
	BySubscriber(cancel <-chan struct{}, a token, id string) (PublicKey, error)

	// Loads the public key by dom
	ByDomain(cancel <-chan struct{}, a token, id string) (PublicKey, error)
}

type InvitationTransport interface {

	// Loads invitations by subscriber and dom
	ById(cancel <-chan struct{}, a token, id uuid.UUID) (Invitation, bool, error)

	// Loads invitations by subscriber
	BySubscriber(cancel <-chan struct{}, a token, subscriber string, beg, end int) ([]Invitation, error)

	// Loads invitations by dom
	ByDomain(cancel <-chan struct{}, a token, dom string, beg, end int) ([]Invitation, error)

	// Loads invitations by subscriber and dom
	BySubscriberAndDomain(cancel <-chan struct{}, a token, subscriber, dom string) (Invitation, bool, error)

	// Registers an invitation with the trust service.
	Upload(cancel <-chan struct{}, a token, i Invitation) error

	// Revokes a certificate.
	Revoke(cancel <-chan struct{}, a token, id uuid.UUID) error
}

type CertTransport interface {

	// Loads active certificates by subscriber
	ActiveBySubscriber(cancel <-chan struct{}, a token, sub string, beg, end int) ([]Certificate, error)

	// Loads active certs by domain.
	ActiveByDomain(cancel <-chan struct{}, a token, dom string, beg, end int) ([]Certificate, error)

	// Loads active cert by (subscriber, domain) tuple.  Only 1 ever allowed.
	ActiveBySubscriberAndDomain(cancel <-chan struct{}, a token, sub, dom string) (Certificate, bool, error)

	// Registers a certificate (and corresponding oracle key).
	Register(cancel <-chan struct{}, a token, c Certificate, k oracleKey, domSig, issSig, truSig Signature) error

	// Revokes a certificate.
	Revoke(cancel <-chan struct{}, a token, id uuid.UUID) error
}

type DomainTransport interface {

	// Loads the domain by id
	ById(cancel <-chan struct{}, a token, id string) (Domain, bool, error)

	// List the domain ids by index
	ByIndex(cancel <-chan struct{}, a token, idx string, beg, end int) ([]string, error)

	// Registers a domain.
	Register(cancel <-chan struct{}, a token, desc string, o oracle, s SigningKey) error
}

package warden

import uuid "github.com/satori/go.uuid"

type PublicKeyTransport interface {

	// Loads public key by subscriber
	BySubscriber(cancel <-chan struct{}, a token, id string) (PublicKey, error)

	// Loads the public key by dom
	ByDomain(cancel <-chan struct{}, a token, id string) (PublicKey, error)
}

type InvitationTransport interface {

	// Loads invitations by subscriber
	BySubscriber(cancel <-chan struct{}, a token, subscriber string, beg, end int) ([]Invitation, error)

	// Loads invitations by dom
	ByDomain(cancel <-chan struct{}, a token, dom string, beg, end int) ([]Invitation, error)

	// Loads invitations by subscriber and dom
	BySubscriberAndDomain(cancel <-chan struct{}, a token, subscriber, dom string) (Invitation, error)

	// Registers an invitation with the trust service.
	Upload(cancel <-chan struct{}, a token, i Invitation) error
}

type CertTransport interface {

	// Loads active certificates by subscriber
	ActiveBySubscriber(cancel <-chan struct{}, a token, sub string, beg, end int) ([]Certificate, error)

	// Loads active certs by domain.
	ActiveByDomain(cancel <-chan struct{}, a token, dom string, beg, end int) ([]Certificate, error)

	// Loads active cert by (subscriber, domain) tuple.  Only 1 ever allowed.
	ActiveBySubscriberAndDomain(cancel <-chan struct{}, a token, sub, dom string) (Certificate, error)

	// Registers a certificate (and corresponding oracle key).
	Register(cancel <-chan struct{}, a token, c Certificate, k oracleKey) error

	// Revokes a certificate.
	Revoke(cancel <-chan struct{}, a token, id uuid.UUID) error
}

type DomainTransport interface {

	// Loads the dom by id
	ById(cancel <-chan struct{}, a token, id string) (Domain, error)

	// List the domain ids by index
	ByIndex(cancel <-chan struct{}, a token, idx string, beg, end int) ([]string, error)

	// Registers a domain.
	Register(cancel <-chan struct{}, a token, desc string, o oracle) error
}

type Transport interface {

	// Returns the subscriber's public key.
	LoadPublicKey(cancel <-chan struct{}, a token, subscriber string) (PublicKey, error)

	// Returns the subscriber's oracle along with the oracle key corresponding to the access method.
	LoadOracle(cancel <-chan struct{}, a token, method string) (oracle, oracleKey, bool, error)

	// Returns the signing key for the owner of the token.
	LoadingSigningKey(cancel <-chan struct{}, a token) (SigningKey, error)

	// Returns the invitation.
	LoadInvitation(cancel <-chan struct{}, a token, id uuid.UUID) (Invitation, bool, error)

	// Returns the dom's public key.
	LoadDomainPublicKey(cancel <-chan struct{}, a token, domKey string) (PublicKey, error)

	// Returns the dom's oracle along with the token owner's access key. (Only returned if the owner is trusted)
	LoadDomainOracle(cancel <-chan struct{}, a token, domKey string) (oracle, oracleKey, error)

	// Returns the dom's encrypted signing key.  (Only returned if trust has been established)
	LoadDomainSigningKey(cancel <-chan struct{}, a token, domKey string) (SigningKey, error)

	// Returns the dom's trust agreements
	LoadCertificatesByDomain(cancel <-chan struct{}, a token, dom string, opts ...func(*PagingOptions)) ([]Certificate, error)

	// Returns the trust certificate between the dom and trustee.
	LoadCertificateByDomainAndTrustee(cancel <-chan struct{}, a token, dom string, trustee string) (Certificate, bool, error)

	// Returns all the certificates for a given subscriber.
	LoadCertificatesBySubscriber(cancel <-chan struct{}, a token, subscriber string, opts ...func(*PagingOptions)) ([]Certificate, error)

	// Revokes the given certificate.  The trustee of the certificate will no longer be authorized
	RevokeCertificate(cancel <-chan struct{}, a token, id uuid.UUID) error

	// Creates a new dom object.  All secure components must be created locally.
	CreateDomain(cancel <-chan struct{}, a token, description string, o oracle, s SigningKey, k oracleKey) (Domain, error)

	// Registers the given invitation.
	RegisterInvitation(cancel <-chan struct{}, a token, i Invitation, domSig Signature, issuerSig Signature) error

	// Registers the certificate
	RegisterCertificate(cancel <-chan struct{}, a token, c Certificate, domSig Signature, issuerSig Signature, trusteeSig Signature, trusteeKey oracleKey) error
}

package warden

import uuid "github.com/satori/go.uuid"

type Transport interface {

	// Returns the account's public verification subscriber.
	LoadSubVerifyKey(cancel <-chan struct{}, a token, subscriber string) (PublicKey, error)

	// Returns the encrypted master subscriber for the given subscription subscriber.
	LoadSubOracle(cancel <-chan struct{}, a token, subscriber string) (oracle, error)

	// Returns the encrypted private subscriber for the given subscription subscriber.
	LoadSubSigningKey(cancel <-chan struct{}, a token, subscriber string) (signingKey, error)

	// Returns the domain's public verification subscriber.
	LoadDomainVerifyKey(cancel <-chan struct{}, a token, domainKey string) (PublicKey, error)

	// Returns the domain's master subscriber.  The master subscriber may be used to decrypt.
	LoadDomainOracle(cancel <-chan struct{}, a token, domainKey string) (oracle, error)

	// Returns the domain's encrypted private subscriber.
	LoadDomainSigningKey(cancel <-chan struct{}, a token, domainKey string) (signingKey, error)

	// Loads the signature.
	LoadSignature(cancel <-chan struct{}, a token, id uuid.UUID) (Signature, error)

	// Returns all the certificates that have been issued by a domain.
	LoadCertificatesByDomain(cancel <-chan struct{}, a token, domain string, beg int, end int) ([]Certificate, error)

	// Returns all the certificates issued for the given subscriber
	LoadCertificatesByDomainAndKey(cancel <-chan struct{}, a token, domain string, subscriber string) ([]Certificate, error)

	// Returns all the certificates for a given subscription subscriber.
	LoadCertificatesByKey(cancel <-chan struct{}, a token, subscriber string) ([]Certificate, error)

	// Revokes the given certificate.  The trustee of the certificate will no longer be authorized
	RevokeCertificate(cancel <-chan struct{}, a token, id uuid.UUID) error

	// Invite(cancel <-chan struct{}, a authToken, i Invitation, domain Signature, trustee Signature) (Invitation, error)
	// Accept(cancel <-chan struct{}, a authToken, i Invitation, recipient Signature) (Certificate, error)
	//
	//
	// LoadInvitiationsByKey(cancel <-chan struct{}, a authToken, subscriber string) ([]Invitation, error)
	// LoadInvitiationsByDomain(cancel <-chan struct{}, a authToken, domain string) ([]Invitation, error)
	//
	//
	// ListDocuments(cancel <-chan struct{}, a authToken, domain string) ([]Document, error)
	// LoadDocument(cancel <-chan struct{}, a authToken, id uuid.UUID) (Document, error)
	// DeleteDocument(cancel <-chan struct{}, a authToken, id uuid.UUID, ver int) error
	// StoreDocument(cancel <-chan struct{}, a authToken, id uuid.UUID, ver int) (Document, error)
}

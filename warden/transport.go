package warden

import uuid "github.com/satori/go.uuid"

type PagingOptions struct {
	Beg int
	End int
}

type Transport interface {

	// Returns the subscriber's public key.
	LoadPublicKey(cancel <-chan struct{}, a token, subscriber string) (PublicKey, error)

	// Returns the subscriber's oracle along with the oracle key corresponding to the access method.
	LoadOracle(cancel <-chan struct{}, a token, method string) (oracle, oracleKey, bool, error)

	// Returns the signing key for the owner of the token.
	LoadingSigningKey(cancel <-chan struct{}, a token) (SigningKey, error)

	// Returns the domain's public key.
	LoadDomainPublicKey(cancel <-chan struct{}, a token, domainKey string) (PublicKey, error)

	// Returns the domain's oracle along with the token owner's access key. (Only returned if the owner is trusted)
	LoadDomainOracle(cancel <-chan struct{}, a token, domainKey string) (oracle, oracleKey, error)

	// Returns the domain's encrypted signing key.  (Only returned if trust has been established)
	LoadDomainSigningKey(cancel <-chan struct{}, a token, domainKey string) (SigningKey, error)

	// Returns the domain's trust agreements
	LoadCertificatesByDomain(cancel <-chan struct{}, a token, domain string, opts ...func(*PagingOptions)) ([]Certificate, error)

	// Returns the trust certificate between the domain and trustee.
	LoadCertificateByDomainAndTrustee(cancel <-chan struct{}, a token, domain string, trustee string) (Certificate, bool, error)

	// Returns all the certificates for a given subscriber.
	LoadCertificatesBySubscriber(cancel <-chan struct{}, a token, subscriber string, opts ...func(*PagingOptions)) ([]Certificate, error)

	// Revokes the given certificate.  The trustee of the certificate will no longer be authorized
	RevokeCertificate(cancel <-chan struct{}, a token, id uuid.UUID) error

	// Creates a new domain object.  All secure components must be created locally.
	CreateDomain(cancel <-chan struct{}, a token, description string, o oracle, s SigningKey, k oracleKey) (Domain, error)

	// Registers the given invitation.
	RegisterInvitation(cancel <-chan struct{}, a token, i Invitation, domainSig Signature, issuerSig Signature) error

	// Registers the certificate
	RegisterCertificate(cancel <-chan struct{}, a token, c Certificate, domainSig Signature, issuerSig Signature, trusteeSig Signature, trusteeKey oracleKey) error
}

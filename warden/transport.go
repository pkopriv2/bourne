package warden

import uuid "github.com/satori/go.uuid"

type authFn func(cancel <-chan struct{}) (auth, error)

type transport struct {
	// Keys    keyTransport
	Certs   certTransport
	Invites inviteTransport
	Trusts  trustTransport
}

// type AuthChallenge struct {
// Msg []byte
// sig Signature
// }
//
// func (a AuthChallenge) Sign(rand io.Reader, k PrivateKey, h Hash) (Signature, error) {
// return Signature{}, nil
// }
//
// type AuthTransport interface {
// WithSignatureInit(id string) ([]byte, Signature, error)
// WithSignatureAuth(id string, s Signature) (authFn, error)
// Register(cancel <-chan struct{}, a authFn, id string) error
// }

type keyTransport interface {

	// Loads public key by subscriber
	BySubscriberAndAlias(cancel <-chan struct{}, a authFn, id uuid.UUID, alias string) (KeyPair, error)

	// Loads public key by subscriber
	PublicBySubscriber(cancel <-chan struct{}, a authFn, id uuid.UUID) (PublicKey, error)

	// Loads the public key by dom
	PublicByTrust(cancel <-chan struct{}, a authFn, id uuid.UUID) (PublicKey, error)
}

type inviteTransport interface {

	// Loads invitations by subscriber and dom
	ById(cancel <-chan struct{}, a authFn, id uuid.UUID) (Invitation, bool, error)

	// Loads invitations by subscriber
	BySubscriber(cancel <-chan struct{}, a authFn, id uuid.UUID, beg, end int) ([]Invitation, error)

	// Loads invitations by dom
	ByTrust(cancel <-chan struct{}, a authFn, dom string, beg, end int) ([]Invitation, error)

	// Loads invitations by subscriber and dom
	BySubscriberAndTrust(cancel <-chan struct{}, a authFn, subscriber, dom string) (Invitation, bool, error)

	// Registers an invitation with the trust service.
	Upload(cancel <-chan struct{}, a authFn, i Invitation) error

	// Revokes a certificate.
	Revoke(cancel <-chan struct{}, a authFn, id uuid.UUID) error
}

type certTransport interface {

	// Loads active certificates by subscriber
	ActiveBySubscriber(cancel <-chan struct{}, a authFn, id uuid.UUID, beg, end int) ([]Certificate, error)

	// Loads active certs by domain.
	ActiveByTrust(cancel <-chan struct{}, a authFn, id uuid.UUID, beg, end int) ([]Certificate, error)

	// Registers a certificate (and corresponding oracle key).
	Register(cancel <-chan struct{}, a authFn, c SignedCertificate, k signedEncryptedShard) error

	// Revokes a certificate.
	Revoke(cancel <-chan struct{}, a authFn, id uuid.UUID) error
}

type trustTransport interface {

	// Loads the domain by id
	ById(cancel <-chan struct{}, a authFn, id uuid.UUID) (Trust, bool, error)

	// Loads the trust by subscriber
	BySubscriber(cancel <-chan struct{}, a authFn, id uuid.UUID, beg, end int) ([]Trust, error)

	// Registers a newly created domain.
	Register(cancel <-chan struct{}, a authFn, dom Trust, sign SignedKeyPair) error
}

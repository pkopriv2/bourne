package warden

import uuid "github.com/satori/go.uuid"

type transport struct {
	Keys    keyTransport
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
// WithSignatureAuth(id string, s Signature) (auth, error)
// Register(cancel <-chan struct{}, a auth, id string) error
// }

type keyTransport interface {

	// Loads public key by subscriber
	BySubscriber(cancel <-chan struct{}, a auth, id uuid.UUID) (PublicKey, error)

	// Loads the public key by dom
	ByTrust(cancel <-chan struct{}, a auth, id uuid.UUID) (PublicKey, error)
}

type inviteTransport interface {

	// Loads invitations by subscriber and dom
	ById(cancel <-chan struct{}, a auth, id uuid.UUID) (Invitation, bool, error)

	// Loads invitations by subscriber
	BySubscriber(cancel <-chan struct{}, a auth, id uuid.UUID, beg, end int) ([]Invitation, error)

	// Loads invitations by dom
	ByTrust(cancel <-chan struct{}, a auth, dom string, beg, end int) ([]Invitation, error)

	// Loads invitations by subscriber and dom
	BySubscriberAndTrust(cancel <-chan struct{}, a auth, subscriber, dom string) (Invitation, bool, error)

	// Registers an invitation with the trust service.
	Upload(cancel <-chan struct{}, a auth, i Invitation) error

	// Revokes a certificate.
	Revoke(cancel <-chan struct{}, a auth, id uuid.UUID) error
}

type certTransport interface {

	// Loads active certificates by subscriber
	ActiveBySubscriber(cancel <-chan struct{}, a auth, id uuid.UUID, beg, end int) ([]Certificate, error)

	// Loads active certs by domain.
	ActiveByTrust(cancel <-chan struct{}, a auth, id uuid.UUID, beg, end int) ([]Certificate, error)

	// Loads active cert by (subscriber, domain) tuple.  Only 1 ever allowed.
	ActiveBySubscriberAndTrust(cancel <-chan struct{}, a auth, sub, dom uuid.UUID) (Certificate, bool, error)

	// Registers a certificate (and corresponding oracle key).
	Register(cancel <-chan struct{}, a auth, c Certificate, k OracleKey, domSig, issSig, truSig Signature) error

	// Revokes a certificate.
	Revoke(cancel <-chan struct{}, a auth, id uuid.UUID) error
}

type trustTransport interface {

	// Loads the domain by id
	ById(cancel <-chan struct{}, a auth, id uuid.UUID) (Trust, bool, error)

	// Registers a newly created domain.
	Register(cancel <-chan struct{}, a auth, dom Trust) error
}

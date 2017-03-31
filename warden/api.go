package warden

import (
	"errors"
	"io"
	"time"

	uuid "github.com/satori/go.uuid"
)

// Useful references:
//
// * https://www.owasp.org/index.php/Key_Management_Cheat_Sheet

// Common errors
var (
	TokenExpiredError = errors.New("Warden:ExpiredToken")
	TokenInvalidError = errors.New("Warden:InvalidToken")
	TrustError        = errors.New("Warden:TrustError")
)

// Starts a session with the trust service.
func Authenticate(addr string, domain string) (KeyPad, error) {
	return nil, nil
}

// Starts the registration process with the trust service.
func Register(addr string, domain string) (KeyPad, error) {
	return nil, nil
}

// A signer contains the knowledge necessary to digitally sign messages.
//
// For high-security environments, only a signer is required to authenticate with the
// system.  It is never necessary for the corresponding private key to leave the local
// system, which is convienent in enviroments where access to the private key is not
// possible (e.g. embedded hardware security modules).
type Signer interface {
	Public() PublicKey
	Sign(rand io.Reader, hash Hash, msg []byte) (Signature, error)
}

// A signature is a cryptographically secure structure that may be used to both prove
// the authenticity of an accompanying document, as well as the identity of the signer.
type Signature struct {
	Hash Hash
	Data []byte
}

// Verifies the signature with the given public key.  Returns nil if the verification succeeded.
func (s Signature) Verify(key PublicKey, msg []byte) error {
	return key.Verify(s.Hash, msg, s.Data)
}

// Public keys are the basis of identity within the trust ecosystem.  In plain english,
// I don't trust your identity, I only trust your keys.  Therefore, risk planning starts
// with limiting the exposure of your trusted keys.  The more trusted a key, the greater
// the implications of a compromised one.
//
// The purpose of using Warden is not just to publish keys for your own personal use
// (although it may be used for that), it is ultimately expected to serve as a way that
// people can form groups of trust.  Every key in the system will be issued a domain
//
type PublicKey interface {
	Algorithm() KeyAlgorithm
	Verify(hash Hash, msg []byte, sig []byte) error
	Encrypt(rand io.Reader, hash Hash, plaintext []byte) ([]byte, error)
	Bytes() []byte
}

// Private keys represent proof of ownership of a public key and form the basis of
// how authentication, confidentiality and integrity concerns are managed within the
// ecosystem.  Warden goes to great lengths to ensure that private keys data are *NEVER*
// derivable by any other actors (malicious or otherwise), including the system itself.
//
type PrivateKey interface {
	Signer
	Algorithm() KeyAlgorithm
	Decrypt(rand io.Reader, hash Hash, ciphertext []byte) ([]byte, error)
	Bytes() []byte
}

// A key pad gives access to the various authentication methods and will
// be used during the registration process.
//
// Future: Accept alternative login methods (e.g. pins, passwords, multi-factor, etc...)
type KeyPad interface {

	// Authenticates using a simple signature scheme. The signer will be asked
	// to sign a simple message, which will then be used as proof that the caller
	// has direct signing access to the private key component of the public key.
	//
	// Note: The public key is only used as a means of performing a simple account
	// lookup and is not used to verify the signature.
	WithSignature(signer Signer) (Token, error)
}

// A token represents an authenticated session with the trust ecosystem.  Tokens
// contain a signed message from the trust service - plus a hashed form of the
// authentication credentials.  The hash is NOT enough to rederive any secrets
// on its own - therefore it is safe to maintain the token in memory, without
// fear of leaking any critical details.
type Token interface {

	// Returns the public key associated with this token.  All activities
	// will be done on behalf of this key.
	Owner(cancel <-chan struct{}) PublicKey

	// Returns the default domain of the owner of this token
	DefaultDomain(cancel <-chan struct{}) (Domain, error)

	// Reconstructs the master key from the token details.  The reconstruction process is such
	// that only the local process has enough knowledge to reconstruct it.  Moreover, the
	// reconstruction process is collaborative, meaning it requires elements from both the
	// user and the trust system.
	//
	// The master key will be promptly destroyed once the given closure returns.
	masterKey(cancel <-chan struct{}, fn func([]byte)) error
}

// A domain represents a group of resources under a private key's control.
type Domain interface {

	// The common identifier of the domain (not required to be uniqued)
	Name() string

	// The public key of the domain.
	PublicKey() PublicKey

	// A short, publicly viewable description of the domain (not advertised, but not public)
	Description() string

	// Loads all the trust certificates that have been issued by this domain
	IssuedCertificates(cancel <-chan struct{}, token Token) ([]Certificate, error)

	// Loads all the trust certificates that have been accepted by this domain
	ReceivedCertificates(cancel <-chan struct{}, token Token) ([]Certificate, error)

	// Loads all the issued invitations that have been issued by this domain
	IssuedInvitations(cancel <-chan struct{}, token Token) ([]Certificate, error)

	// Loads all the pending invitations that have been issued to this domain
	ReceivedInvitations(cancel <-chan struct{}, token Token) ([]Certificate, error)

	// Lists all the document names under the control of this domain
	ListDocument(cancel <-chan struct{}, token Token) ([]string, error)

	// Loads a specific document.
	LoadDocument(cancel <-chan struct{}, token Token, name []byte) (struct{}, error)

	// Stores a document under the domain
	StoreDocument(cancel <-chan struct{}, token Token, name []byte, ver int) (struct{}, error)

	// Stores a document under the domain
	DeleteDocument(cancel <-chan struct{}, token Token, name []byte, ver int) (struct{}, error)
}

// Trust levels dictate the terms for how a secret may be used once established.
type Level int

const (
	Use Level = iota
	Issue
	Revoke
)

// A certificate is a receipt that trust has been established.
type Certificate struct {
	Id        uuid.UUID
	Issuer    string
	Trustee   string
	Level     Level
	IssuedAt  time.Time
	StartsAt  time.Time
	ExpiresAt time.Time
}

// Returns a consistent byte representation of a certificate
func (c Certificate) Bytes() []byte {
	return nil
}

// A signed certificate is a receipt + proof that trust has been established.
type SignedCertificate struct {
	Certificate
	IssuerSignature  Signature
	TrusteeSignature Signature
}

func (i SignedCertificate) Verify(issuer PublicKey, trustee PublicKey) error {
	return nil
}

// An invitation is a cryptographically secured message asking the recipient to share in the
// management of a domain. The invitation may only be accepted by the intended recipient.
// These technically can be shared publicly, but exposure should be limited (typically only the
// trust system needs to know).
type Invitation struct {
	Id        uuid.UUID
	Issuer    string
	Trustee   string
	Level     Level
	Starts    time.Time
	Duration  time.Duration
	Signature Signature
	payload   []byte
}

func (i Invitation) Bytes() []byte {
	return nil
}

func (i Invitation) Verify(key PublicKey) error {
	return nil
}

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
	SessionExpiredError  = errors.New("Warden:ExpiredSession")
	SessionInvalidError  = errors.New("Warden:InvalidSession")
	TrustError           = errors.New("Warden:TrustError")
	DomainInvariantError = errors.New("Warden:DomainInvariantError")
)

// Registers a new subscription with the trust service.
func NewSubscription(addr string) (KeyPad, error) {
	return nil, nil
}

// Loads a subscription.
func LoadSubscription(addr string) (KeyPad, error) {
	return nil, nil
}

// Loads the private domain.
func ManagePrivateDomain(session Session) (Domain, error) {
	return nil, nil
}

// Registers/creates the domain.  Must not already exist.
func RegisterDomain(session Session, domain string) (Domain, error) {
	return nil, nil
}

// Publishes the domain to the main index.  It will now be globally searchable.
func PublishDomain(session Session, domain string) ([]Domain, error) {
	return nil, nil
}

// Lists all the domain on the main index.
func ListDomains(session Session, beg int, end int) ([]Domain, error) {
	return nil, nil
}

// Loads the domain with the given name.  The domain will be returned only
// if your public key has been invited to join the domain and the invitation
// has been accepted.
func ManageDomain(session Session, domain string) (Domain, error) {
	return nil, nil
}

// Lists a subsection of keys from [beg,end].  Boundaries given for client pagination
func ListKeys(session Session, beg int, end int) ([]string, error) {
	return nil, nil
}

// Requests an invite to the domain.
func RequestInvite(session Session, domain string) error {
	return nil
}

// Lists your session's currently pending invitations.  These are invitations to manage in other domains.
func ListInvitations(session Session) ([]Invitation, error) {
	return nil, nil
}

// Verifies the contents of an invitation.
func VerifyInvitation(session Session, invite Invitation) error {
	return nil
}

// Accepts the invitation with the given id.  Returns an error if the
// acceptance fails for any reason.
func AcceptInvite(session Session, id uuid.UUID) error {
	return nil
}

// Lists your session's key's currently outstanding trust invitations.
func ListCertificates(session Session) ([]Certificate, error) {
	return nil, nil
}

// Verifies the contents of a certificate.
func VerifyCertificate(session Session, cert Certificate) error {
	return nil
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
	WithSignature(signer Signer) (Session, error)
}

// A session represents an authenticated session with the trust ecosystem.  Sessions
// contain a signed message from the trust service - plus a hashed form of the
// authentication credentials.  The hash is NOT enough to rederive any secrets
// on its own - therefore it is safe to maintain the session in memory, without
// fear of leaking any critical details.
type Session interface {

	// Returns the public key associated with this session.  All activities
	// will be done on behalf of this key.
	Owner(cancel <-chan struct{}) PublicKey

	// The universe to which this session belongs.  The multiverse of trust
	// servers is used to physically isolate clients who have paid for the
	// additional security.
	universe() string

	// Reconstructs the master key from the session details.  The reconstruction process is such
	// that only the local process has enough knowledge to reconstruct it.  Moreover, the
	// reconstruction process is collaborative, meaning it requires elements from both the
	// user and the trust system.
	//
	// The master key will be promptly destroyed once the given closure returns.
	masterKey(cancel <-chan struct{}, fn func([]byte)) error
}

// Trust levels dictate the terms for what actions a user can take on a domain.
type LevelOfTrust int

const (
	DocumentVerify LevelOfTrust = iota + 10
	DocumentLoad
	DocumentUpdate
	DocumentSign
	Invite
	Revoke
)

// A domain represents a group of documents under the control of a single (possibly shared) private key.
//
// You may access a domain only if you have established trust.
type Domain interface {

	// The common identifier of the domain.  Must be unique per universe. (FIXME: Is uniqueness necessary.)
	Name() string

	// The public key of the domain.
	PublicKey() PublicKey

	// A short, publicly viewable description of the domain (not advertised, but not public)
	Description() string

	// Loads all the trust certificates that have been issued by this domain
	IssuedCertificates(cancel <-chan struct{}, session Session) ([]Certificate, error)

	// Revokes a certificate.  The trustee will no longer be able to act in the management of the domain.
	RevokeCertificate(cancel <-chan struct{}, session Session, id uuid.UUID) error

	// Loads all the issued invitations that have been issued by this domain
	IssuedInvitations(cancel <-chan struct{}, session Session) ([]Invitation, error)

	// Issues an invitation to the given key.
	Invite(cancel <-chan struct{}, session Session, key string, level LevelOfTrust, ttl time.Duration) (Invitation, error)

	// Lists all the document names under the control of this domain
	ListDocument(cancel <-chan struct{}, session Session) ([]string, error)

	// Loads a specific document.
	LoadDocument(cancel <-chan struct{}, session Session, name []byte) (struct{}, error)

	// Stores a document under the domain
	StoreDocument(cancel <-chan struct{}, session Session, name []byte, ver int) (struct{}, error)

	// Stores a document under the domain
	DeleteDocument(cancel <-chan struct{}, session Session, name []byte, ver int) (struct{}, error)
}

// A certificate is a receipt that trust has been established.
type Certificate struct {
	Id      uuid.UUID
	Issuer  string
	Trustee string
	Level   LevelOfTrust

	IssuedAt  time.Time
	StartsAt  time.Time
	ExpiresAt time.Time
}

// Returns a consistent byte representation of a certificate
func (c Certificate) Bytes() []byte {
	return nil
}

// An invitation is a cryptographically secured message asking the recipient to share in the
// management of a domain. The invitation may only be accepted by the intended recipient.
// These technically can be shared publicly, but exposure should be limited (typically only the
// trust system needs to know).
type Invitation struct {
	Id      uuid.UUID
	Issuer  string
	Trustee string
	Level   LevelOfTrust

	IssuedAt  time.Time
	StartsAt  time.Time
	ExpiresAt time.Time

	payload []byte
}

func (i Invitation) Bytes() []byte {
	return nil
}

func (i Invitation) decrypt(key PrivateKey) ([]byte, error) {
	return nil, nil
}

func (i Invitation) Sign(key PrivateKey, hash Hash) (Signature, error) {
	return Signature{}, nil
}

type Document struct {
	Id         uuid.UUID
	DomainId   uuid.UUID
	StartBlock uuid.UUID
	CreatedAt  time.Time
	Contents   []byte
}

func (d Document) Bytes() error {
	return nil
}

func (d Document) Sign(key PrivateKey, hash Hash) (Signature, error) {
	return Signature{}, nil
}

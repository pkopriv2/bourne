package warden

import (
	"io"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// Useful references:
//
// * https://www.owasp.org/index.php/Key_Management_Cheat_Sheet

// Common errors
var (
	SessionExpiredError  = errors.New("Warden:ExpiredSession")
	SessionInvalidError  = errors.New("Warden:InvalidSession")
	DomainInvariantError = errors.New("Warden:DomainInvariantError")
	TrustError           = errors.New("Warden:TrustError")
)

// Registers a new subscription with the trust service.
func Subscribe(addr string) (KeyPad, error) {
	return nil, nil
}

// Loads a subscription.
func Connect(addr string) (KeyPad, error) {
	return nil, nil
}

// Lists a subsection of public key ids from [beg,end).
func ListKeys(session Session, beg int, end int) ([]string, error) {
	return nil, nil
}

// Loads the public key.
func LoadKey(session Session, id string) (PublicKey, error) {
	return nil, nil
}

// Publishes the key to the main key index using the given name.  You must offer proof of ownership
func PublishKey(session Session, id string, name string) error {
	return nil
}

// Verifies that the key is indeed an authentic key, registered with the system.
func VerifyKey(session Session, pub PublicKey) (PublicKey, error) {
	return nil, nil
}

// Registers/creates a domain.  The domain will be controlled by newly generated public/private key
// pair and a certificate of trust will be issued to the session's owner.
func CreateDomain(session Session) (Domain, error) {
	return Domain{}, nil
}

// Lists all the domains created/trusted by the session's owner.
func ListPrivateDomains(session Session, beg int, end int) ([]Domain, error) {
	return nil, nil
}

// Publishes the domain to the main index using the given name.  It will now be globally searchable.
func PublishDomain(session Session, id string, name string) error {
	return nil
}

// Lists all the domains that have been published on the main index.
func ListPublishedDomains(session Session, beg int, end int) ([]Domain, error) {
	return nil, nil
}

// Loads the domain with the given name.  The domain will be returned only
// if your public key has been invited to manage the domain and the invitation
// has been accepted.
func LoadDomain(session Session, id string) (Domain, bool, error) {
	return Domain{}, false, nil
}

// Requests an invite to the domain of the given id.
func RequestInvite(session Session, id string, level LevelOfTrust) error {
	return nil
}

// Lists your session's currently pending invitations.
func ListInvitations(session Session) ([]Invitation, error) {
	return nil, nil
}

// Accepts the invitation with the given id.  Returns an error if the
// acceptance fails for any reason.
func AcceptInvite(session Session, id uuid.UUID) error {
	return nil
}

// Verifies the contents of an invitation.
func VerifyInvitation(session Session, invite Invitation) error {
	return nil
}

// Lists your session's currently outstanding trust invitations.
func LoadCertificate(session Session, id uuid.UUID) (Certificate, bool, error) {
	return Certificate{}, false, nil
}

// Lists all the certificates of trust that have been issued to the given session.
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
// people can form groups of trust.
//
type PublicKey interface {
	Id() string
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


// Trust levels dictate the terms for what actions a user can take on a domain.
type LevelOfTrust int

const (
	DocumentVerify LevelOfTrust = iota + 10
	DocumentLoad
	DocumentUpdate
	DocumentSign
	Invite
	Revoke
	Publish
	Destroy
)


// A certificate is a receipt that trust has been established.
type Certificate struct {
	Id      uuid.UUID
	Issuer  string
	Trustee string

	Level LevelOfTrust

	IssuedAt  time.Time
	StartsAt  time.Time
	ExpiresAt time.Time

	// not part of signed contents
	issuerSignature  uuid.UUID
	trusteeSignature uuid.UUID
}

// Verifies that the signature matches the certificate contents.
func (c Certificate) Verify(key PublicKey, signature Signature) error {
	return nil
}

// Signs the certificate with the private key.
func (c Certificate) Sign(key PrivateKey, hash Hash) (Signature, error) {
	return Signature{}, nil
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
	Domain  string
	Issuer  string
	Trustee string

	Level LevelOfTrust

	IssuedAt  time.Time
	StartsAt  time.Time
	ExpiresAt time.Time

	payload []byte
}

func (i Invitation) Bytes() []byte {
	return nil
}

// Verifies that the signature matches the certificate contents.
func (c Invitation) Verify(key PublicKey, signature Signature) error {
	return nil
}

// Signs the certificate with the private key.
func (c Invitation) Sign(key PrivateKey, hash Hash) (Signature, error) {
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

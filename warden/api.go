package warden

import (
	"io"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// FIXMES:
//	* Get off gob encoding/decoding so we can easily support other languages.


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

// The paging options
type PagingOptions struct {
	Beg int
	End int
}

// Constructs paging options.
func buildPagingOptions(fns ... func(p *PagingOptions)) PagingOptions {
	opts := PagingOptions{0, 256}
	for _, fn := range fns {
		fn(&opts)
	}
	return opts
}

// Registers a new subscription with the trust service.
func Subscribe(addr string) (KeyPad, error) {
	return nil, nil
}

// Loads a subscription.
func Connect(addr string) (KeyPad, error) {
	return nil, nil
}

// Publishes the key to the main key index using the given name
func PublishMyKey(session Session) error {
	return nil
}

// Lists a subsection of public key ids from [beg,end).
func ListPublishedKeys(cancel <-chan struct{}, session Session, fns ...func(*PagingOptions)) ([]string, error) {
	return nil, nil
}

// Loads the public key of the given subscriber
func LoadSubscriberKey(session Session, id string) (PublicKey, error) {
	return nil, nil
}

// Registers/creates a domain.  The domain will be controlled by newly generated public/private key
// pair and a certificate of trust will be issued to the session's owner.
func CreateDomain(session Session) (Domain, error) {
	return Domain{}, nil
}

// Lists all the domains created/trusted by the session's owner.
func ListPrivateDomains(session Session, fns ...func(*PagingOptions)) ([]Domain, error) {
	return nil, nil
}

// Publishes the domain to the main index using the given name.  It will now be globally searchable.
func PublishDomain(session Session, id string, name string) error {
	return nil
}

// Lists all the domains that have been published on the main index.
func ListPublishedDomains(session Session, fns ...func(*PagingOptions)) ([]Domain, error) {
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
func AcceptInvite(s Session, id uuid.UUID) error {
	return nil
}

// Verifies the contents of an invitation.
func VerifyInvitation(cancel <-chan struct{}, session Session, invite Invitation) error {
	auth, err := session.auth(cancel)
	if err != nil {
		return errors.WithStack(err)
	}

	domainKey, err := session.net.LoadPublicKey(cancel, auth, invite.Cert.Domain)
	if err != nil {
		return errors.WithStack(err)
	}

	issuerKey, err := session.net.LoadPublicKey(cancel, auth, invite.Cert.Issuer)
	if err != nil {
		return errors.WithStack(err)
	}

	if err := invite.Cert.Verify(domainKey, invite.DomainSig); err != nil {
		return errors.Wrapf(
			err, "Error verify certificate with domain key [%v]", invite.Cert.Domain)
	}

	if err := invite.Cert.Verify(issuerKey, invite.IssuerSig); err != nil {
		return errors.Wrapf(
			err, "Error verify certificate with domain key [%v]", invite.Cert.Issuer)
	}

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
// derivable by any untrusted actors (malicious or otherwise), including the system itself.
//
type PrivateKey interface {
	Signer
	Algorithm() KeyAlgorithm
	Decrypt(rand io.Reader, hash Hash, ciphertext []byte) ([]byte, error)
	Bytes() []byte
	Destroy()
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

type Document struct {
	Id       uuid.UUID
	Name     string
	Ver      int
	Created  time.Time
	Contents []byte
}

func (d Document) Bytes() error {
	return nil
}

func (d Document) Sign(key PrivateKey, hash Hash) (Signature, error) {
	return Signature{}, nil
}

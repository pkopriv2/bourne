package warden

import (
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

// FIXMES:
//	* Get off gob encoding/decoding so we can easily support other languages.

// Useful references:
//
// * https://www.owasp.org/index.php/Key_Management_Cheat_Sheet
// * Secret sharing survey:
//		* https://www.cs.bgu.ac.il/~beimel/Papers/Survey.pdf
// * Computational secrecy:
//		* http://www.cs.cornell.edu/courses/cs754/2001fa/secretshort.pdf
// * SSRI (Secure store retrieval and )
//		* http://ac.els-cdn.com/S0304397598002631/1-s2.0-S0304397598002631-main.pdf?_tid=d4544ec8-221e-11e7-9744-00000aacb35f&acdnat=1492290326_2f9f40490893fb853da9d080f5b47634
// * Blakley's scheme
//		* https://en.wikipedia.org/wiki/Secret_sharing#Blakley.27s_scheme

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
func buildPagingOptions(fns ...func(p *PagingOptions)) PagingOptions {
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

// Loads a subscription
func Connect(addr string) (KeyPad, error) {
	return nil, nil
}

// Creates a local domain.
func CreateDomain(cancel <-chan struct{}, s Session, desc string, fns ...func(*DomainOptions)) (Domain, error) {
	auth, err := s.auth(cancel)
	if err != nil {
		return Domain{}, errors.WithStack(err)
	}

	dom, err := generateDomain(s, desc, fns...)
	if err != nil {
		return Domain{}, errors.WithStack(err)
	}

	if err := s.net.Domains.Register(cancel, auth, dom); err != nil {
		return Domain{}, errors.WithStack(err)
	}

	return dom, nil
}

// Lists all the domains that have been published on the main index.
func ListDomains(cancel <-chan struct{}, s Session, fns ...func(*PagingOptions)) ([]string, error) {
	opts := buildPagingOptions(fns...)

	auth, err := s.auth(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return s.net.Domains.ByIndex(cancel, auth, s.myIndex(), opts.Beg, opts.End)
}

// Loads the domain with the given name.  The domain will be returned only
// if your public key has been invited to manage the domain and the invitation
// has been accepted.
func LoadDomain(cancel <-chan struct{}, s Session, id string) (Domain, bool, error) {
	auth, err := s.auth(cancel)
	if err != nil {
		return Domain{}, false, errors.WithStack(err)
	}

	return s.net.Domains.ById(cancel, auth, id)
}

// Lists the session owner's currently pending invitations.
func ListInvitations(cancel <-chan struct{}, s Session, fns ...func(*PagingOptions)) ([]Invitation, error) {
	auth, err := s.auth(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	opts := buildPagingOptions(fns...)
	return s.net.Invites.BySubscriber(cancel, auth, s.MyId(), opts.Beg, opts.End)
}

// Accepts the invitation with the given id.  Returns an error if the
// acceptance fails for any reason.
//
// TODO: verify invitation prior to accepting.
func AcceptInvite(cancel <-chan struct{}, s Session, id uuid.UUID) error {
	auth, err := s.auth(cancel)
	if err != nil {
		return errors.WithStack(err)
	}

	inv, ok, err := s.net.Invites.ById(cancel, auth, id)
	if err != nil || !ok {
		return errors.WithStack(common.Or(err, DomainInvariantError))
	}

	dom, ok, err := s.net.Domains.ById(cancel, auth, inv.Cert.Domain)
	if err != nil || !ok {
		return errors.WithStack(err)
	}

	priv, err := s.MySigningKey()
	if err != nil {
		return errors.WithStack(err)
	}

	key, err := acceptInvitation(s.rand, inv, dom.oracle, priv, s.myOracle(), dom.oracle.Opts)
	if err != nil {
		return errors.WithStack(err)
	}

	sig, err := inv.Cert.Sign(s.rand, priv, dom.oracle.Opts.SigHash)
	if err != nil {
		return errors.WithStack(err)
	}

	return s.net.Certs.Register(cancel, auth, inv.Cert, key, inv.DomainSig, inv.IssuerSig, sig)
}

// Verifies the contents of an invitation.
func VerifyInvitation(cancel <-chan struct{}, s Session, i Invitation) error {
	auth, err := s.auth(cancel)
	if err != nil {
		return errors.WithStack(err)
	}

	domainKey, err := s.net.Keys.ByDomain(cancel, auth, i.Cert.Domain)
	if err != nil {
		return errors.WithStack(err)
	}

	issuerKey, err := s.net.Keys.BySubscriber(cancel, auth, i.Cert.Issuer)
	if err != nil {
		return errors.WithStack(err)
	}

	if err := i.Cert.Verify(domainKey, i.DomainSig); err != nil {
		return errors.Wrapf(
			err, "Error verify certificate with domain key [%v]", i.Cert.Domain)
	}

	if err := i.Cert.Verify(issuerKey, i.IssuerSig); err != nil {
		return errors.Wrapf(
			err, "Error verify certificate with domain key [%v]", i.Cert.Issuer)
	}

	return nil
}

// Lists all the certificates of trust that have been issued to the given session.
func ListCertificates(cancel <-chan struct{}, s Session, fns ...func(*PagingOptions)) ([]Certificate, error) {
	opts := buildPagingOptions(fns...)

	auth, err := s.auth(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return s.net.Certs.ActiveBySubscriber(cancel, auth, s.MyId(), opts.Beg, opts.End)
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

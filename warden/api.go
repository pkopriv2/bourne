package warden

import (
	"fmt"
	"io"

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
	RpcError           = errors.New("Warden:RpcError")
	TokenExpiredError  = errors.New("Warden:TokenExpired")
	TokenInvalidError  = errors.New("Warden:TokenInvalid")
	UnauthorizedError  = errors.New("Warden:Unauthorized")
	InvariantError     = errors.New("Warden:InvariantError")
	AuthError          = errors.New("Warden:AuthError")
	TrustError         = errors.New("Warden:TrustError")
	UnknownMemberError = errors.New("Warden:UnknownMember")
	UnknownTrustError  = errors.New("Warden:UnknownTrust")
)

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

// A a signable object is one that has a consistent format for signing and verifying.
//
// FIXME: Warden currently relies heavily on formats which should be language independent.
type Signable interface {
	SigningFormat() ([]byte, error)
}

// A signature is a cryptographically secure structure that may be used to both prove
// the authenticity of an accompanying document, as well as the identity of the signer.
type Signature struct {
	Key  string
	Hash Hash
	Data []byte
}

// Verifies the signature with the given public key.  Returns nil if the verification succeeded.
func (s Signature) Verify(key PublicKey, msg []byte) error {
	return key.Verify(s.Hash, msg, s.Data)
}

// Returns a simple string rep of the signature.
func (s Signature) String() string {
	return fmt.Sprintf("Signature(hash=%v): %v", s.Hash, cryptoBytes(s.Data))
}

// A decrypter decrypts itself with the given key
type Encrypted interface {
	Decrypt(key []byte) ([]byte, error)
}

// A destroyer simply destroys itself.
type Destroyer interface {
	Destroy()
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
	format() []byte
}

// Private keys represent proof of ownership of a public key and form the basis of
// how authentication, confidentiality and integrity concerns are managed within the
// ecosystem.  Warden goes to great lengths to ensure that private keys data are *NEVER*
// derivable by any untrusted actors (malicious or otherwise), including the system itself.
//
type PrivateKey interface {
	Signer
	Destroyer

	Algorithm() KeyAlgorithm
	Decrypt(rand io.Reader, hash Hash, ciphertext []byte) ([]byte, error)
	format() []byte
}

// Membership registration
type Registrar interface {

	// Registers an account under an email address. (Email really is a terrible mechanism
	// for handling registration, but I don't think we can tackle that....)
	RegisterByEmail(email string) KeyPad

	// Registers an account using a public key as the primary lookup mechanism.  An alternative
	// lookup (e.g. email may be registered later, but is not required).
	RegisterByKey(key PublicKey) KeyPad
}

// Membership lookup (Used during authentication)
type Directory interface {

	// Lookup a member account by public key
	LookupByKey(key PublicKey) KeyPad

	// Lookup a member account by email
	LookupByEmail(email string) KeyPad
}

// KeyPad is used to provide a session with credential information.
type KeyPad interface {

	// Performs a signature based login/registration.
	EnterSigner(signer Signer, strength ...Strength) (Session, error)

	// Performs a passphrase based login/registration.  The passphrase is
	// hashed and promptly destroyed.  The phrase will be protected even
	// in the case that the machine is compromised.  However, a hashed
	// form may be provided to the server.
	EnterPassphrase(phrase string) (Session, error)
}

// Session
type Session interface {
	io.Closer

	// Returns the subscriber id associated with this session.  This uniquely identifies
	// an account to the world.  This may be shared over other (possibly unsecure) channels
	// in order to share with other users.
	MyId() uuid.UUID

	// Returns the session owner's public signing key.  This key and its id may be shared freely.
	MyKey() PublicKey

	// Returns the session owner's current subscription role.
	MyRole() Role

	// Returns the session owner's unaccepted invitations.
	MyInvitations(cancel <-chan struct{}, fns ...func(*PagingOptions)) ([]Invitation, error)

	// Lists the session owner's currently active trust certificates.
	MyCertificates(cancel <-chan struct{}, fns ...func(*PagingOptions)) ([]SignedCertificate, error)

	// Trusts that have been created by the session owner.
	MyTrusts(cancel <-chan struct{}, fns ...func(*PagingOptions)) ([]Trust, error)

	// Generates a new trust.
	GenerateTrust(cancel <-chan struct{}, strength Strength) (Trust, error)

	// Loads a trust by id.
	LoadTrust(cancel <-chan struct{}, id uuid.UUID) (Trust, bool, error)

	// Invites a member to join the trust.
	Invite(cancel <-chan struct{}, trust Trust, memberId uuid.UUID, opts ...func(*InvitationOptions)) (Invitation, error)

	// Accepts the invitation.  The invitation must be valid and must be addressed
	// to the owner of the session, or the session owner must be acting as a proxy.
	Accept(cancel <-chan struct{}, invitationId Invitation) error

	// Revokes trust from the given subscriber for the given trust.
	Revoke(cancel <-chan struct{}, trust Trust, memberId uuid.UUID) error

	// // Temporarily revokes trust from the given subscriber for the given trust.
	// Disable(cancel <-chan struct{}, trust Trust, memberId uuid.UUID) error

	// Renew's the session owner's certificate with the trust.
	Renew(cancel <-chan struct{}, trust Trust) (Trust, error)

	// Loads the certificates for the given trust.
	LoadCertificates(cancel <-chan struct{}, trust Trust, fns ...func(*PagingOptions)) ([]SignedCertificate, error)

	// Loads the invitations for the given trust.
	LoadInvitations(cancel <-chan struct{}, trust Trust, fns ...func(*PagingOptions)) ([]Invitation, error)

}

// Registers a new subscription with the trust service.
func Register(ctx common.Context, addr string, token SignedToken, fns ...func(*SessionOptions)) (Registrar, error) {
	opts := buildSessionOptions(fns...)
	if opts.deps == nil {
		deps, err := buildDependencies(ctx, addr, opts.Timeout)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		opts.deps = &deps
	}
	return &registrar{ctx, opts, token}, nil
}

// Registers a new subscription with the trust service.
func Connect(ctx common.Context, addr string, fns ...func(*SessionOptions)) (Directory, error) {
	opts := buildSessionOptions(fns...)
	if opts.deps == nil {
		deps, err := buildDependencies(ctx, addr, opts.Timeout)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		opts.deps = &deps
	}
	return &directory{ctx, opts}, nil
}

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

// Signs the object with the signer and hash
func sign(rand io.Reader, obj Signable, signer Signer, hash Hash) (Signature, error) {
	fmt, err := obj.SigningFormat()
	if err != nil {
		return Signature{}, errors.WithStack(err)
	}

	sig, err := signer.Sign(rand, hash, fmt)
	if err != nil {
		return Signature{}, errors.WithStack(err)
	}
	return sig, nil
}

// Verifies the object with the signer and hash
func verify(obj Signable, key PublicKey, sig Signature) error {
	fmt, err := obj.SigningFormat()
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(key.Verify(sig.Hash, fmt, sig.Data))
}

// Decrypts the object with the key.
func decrypt(obj Encrypted, key Signable) ([]byte, error) {
	fmt, err := key.SigningFormat()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ret, err := obj.Decrypt(fmt)
	return ret, errors.WithStack(err)
}

package warden

import (
	"errors"
	"io"

	uuid "github.com/satori/go.uuid"
)

// TODO: Follow the guidelines here, esp NIST standards: https://www.owasp.org/index.php/Key_Management_Cheat_Sheet

// Common errors
var (
	SessionExpiredError = errors.New("Warden:ExpiredSession")
	SessionInvalidError = errors.New("Warden:InvalidSession")
	TrustError          = errors.New("Warden:TrustError")
)

// A signer contains the knowledge necessary to digitally sign messages.
type Signer interface {
	Public() PublicKey
	Sign(rand io.Reader, hsh Hash, msg []byte) (Signature, error)
}

// A signature is a 3-tuple of the hashing algorithm, the original message
// and the signature bytes.  Signatures may be verified with a public key,
// preferably obtained through a trusted source.
//
// TODO: Not sure if this struct is necessary.
type Signature struct {
	Alg Hash
	Msg []byte
	Sig []byte
}

// Verifies the signature with the given public key.  Returns nil if the
// verification succeeded.
func (s Signature) Verify(key PublicKey) error {
	return key.Verify(s.Alg, s.Msg, s.Sig)
}

// Public keys are the basis of identity within the trust ecosystem.  In plain english,
// I don't trust your identity, I only trust your keys.  Therefore, risk planning starts
// with limiting the exposure of your trusted keys.  The more trusted a key, the greater
// the implications of a compromised one.
//
// A couple initial thoughts on key organization for users.  I personally plan to
// issue my keys in terms of the trust required.
//
// For development purposes, I think issuing keys based on the environment
// I'm working on will prove best, e.g. dev, cert, prod, etc...
//
type PublicKey interface {
	Algorithm() KeyAlgorithm
	Verify(hsh Hash, msg []byte, sig []byte) error
	Bytes() []byte
}

// Private keys represent proof of ownership of a public key and form the basis of
// how authentication confidentiality and integrity concerns are managed within the
// ecosystem.  Warden goes to great lengths to ensure that your data is *NEVER*
// derivable by any other actors (malicious or otherwise), including the system itself.
//
// For high-security private keys, only a signer is required to authenticate with the
// system.  It is never necessary for private key to leave the local system, which is
// convienent in enviroments where access to the private key is not possible
// (e.g. embedded hardware security modules).
//
// For user-defined keys, users may wish to store encrypted backups of their private
// keys.  The plaintext version of these keys (PrivateKey#Bytes()) never leave the
// the local machine, ensuring that the trust ecosystem has no knowledge of the key
// values.  Consumers may access their keys using their key ring.
//
type PrivateKey interface {
	Signer
	Algorithm() KeyAlgorithm
	Bytes() []byte
}

// A key pad gives access to the various authentication methods.
type KeyPad interface {

	// Authenticates using a simple signature scheme. The signer will be asked
	// to sign a simple message, which will then be used as proof that the caller
	// has direct signing access to the private key component of the public key.
	//
	// Note: The public key is only used as a means of performing a simple account
	// lookup and is not used to verify the signature.
	WithSignature(account string, signer Signer) (Token, error)

	// // Authenticates with a simple user-generated pin.
	// WithPin(user string, pin []byte) (Session, error)
	//
	// // Authenticates using the default username/password combo.  We currently
	// // do not support multifactor auth, so this should be considered the weakest
	// // of the 3, but does not require that the user has direct access to his/her
	// // private key.
	// WithPassword(user string, pass []byte)
}

// A token represents an authenticated session with the trust ecosystem.  Tokens
// contain a signed message from the trust service - plus a hashed form of the
// authentication credentials.  The hash is NOT enough to rederive any secrets
// on its own.
type Token interface {

	// Returns the public key associated with the token.
	PublicKey(cancel <-chan struct{}) PublicKey

	// Reconstructs the secret key from the token details.  The reconstruction process is such
	// that only the local process has enough knowledge to reconstruct it.  Moreover, the
	// reconstruction process is collaborative, meaning it requires elements from both the
	// user and the trust system.
	//
	// The private key will be promptly destroyed once the given closure returns.
	secretKey(cancel <-chan struct{}, fn func([]byte)) error
}

// A key ring hosts a set of key/pairs.
type KeyRing interface {

	// The owning key.
	Owner() PublicKey

	// Loads the existing invitations for the key of the given name
	Invitations(cancel <-chan struct{}, token Token, name string) ([]Invitation, error)

	// Loads the public key of the given name.
	PublicKey(cancel <-chan struct{}, token Token, name string) (PublicKey, error)

	// Loads the private key of the given name.
	PrivateKey(cancel <-chan struct{}, token Token, name string) (PrivateKey, error)

	// Publishes a key to the key ring.  Although not searchable, the key should be
	// considered public knowledge.
	Publish(cancel <-chan struct{}, token Token, name string, key PublicKey) error

	// Marks the given key as being compromised.  This will be published to a
	// global revocation list that consumers will have access to check.
	Revoke(cancel <-chan struct{}, token Token, name string) error

	// Backs the private key up.  The plaintext private key never leaves
	// the local machine, but an encrypted form will be sent to the trust
	// key store.  The encrypted key will only be accessible to the owner
	// of the key ring.
	Backup(cancel <-chan struct{}, token Token, name string, key PrivateKey) error
}

// A secret is a durable, cryptographically secure structure that protects a long-lived secret.
//
// A secret should be considered private to its owner and MUST NEVER BE SHARED. It has been
// constructed in such a way that only the owner has the knowledge required to recover it.
//
// The plaintext form of the secret never leaves the local process space and any time a secret
// is in transit or is durably stored, it is done so in an encrypted form that is only recoverable
// by its owner.
//
type Secret interface {

	// Returns the unique identifier for the secret.
	Id() uuid.UUID

	// Applies the given closure to the plaintext secret.
	//
	// Extreme care should be taken NOT to leak the secret beyond the boundaries of the
	// closure.  By convention, the secret is promptly destroyed once the closure returns
	// and the plaintext secret is never distributed to any 3rd party systems.
	Use(cancel <-chan struct{}, token Token, fn func([]byte)) error
}

// An invitation is a cryptographically secured message that may only be accepted
// by the intended recipient.  These technically can be shared publicly, but exposure
// should be limited (typically only the trust system needs to know)
type Invitation interface {
	Group() PublicKey

	// Accepts the invitation on behalf of the owner of the token.
	Accept(cancel <-chan struct{}, token Token) error
}

// Group levels set the basic policy for members of a trusted group to make changes to the group.
type TrustLevel int

const (
	Verify TrustLevel = iota
	Sign
	Encrypt
	Decrypt
	Invite
	Evict
	Disband
)

// A group represents a group of keys that have all been trusted for the purposes of maintaining
// a shared group secret.
type Group interface {

	// The key that originally created the group.
	Creator() PublicKey

	// The global name of the group.  This must be unique.  This is not published
	// but should be considered public knowledge.
	//
	// Consumers will use the uri to coordinate invitations.
	URI() string

	// Returns all the currently trusted keys of the group
	Trusted(cancel <-chan struct{}, token Token) []PublicKey

	// Disbands the group.  The group's private key will be permanently irrecoverable.
	Disband(cancel <-chan struct{}, token Token) error

	// Generates an invitation for the given public key to join the group.
	Invite(cancel <-chan struct{}, token Token, key PublicKey, lvl TrustLevel) (Invitation, error)

	// Generates an invitation request for the given public key to join the group.
	RequestInvitation(cancel <-chan struct{}, token Token, key PublicKey, lvl TrustLevel) error

	// Generates an invitation for the given public key to join the group.
	Evict(cancel <-chan struct{}, token Token, key PublicKey) error

	// Verifies the signature was generated from the group's private key and the msg.
	Verify(cancel <-chan struct{}, token Token, hash Hash, msg []byte, sig []byte) ([]byte, error)

	// Signs the message with the group's private key.
	Sign(cancel <-chan struct{}, token Token, hash Hash, msg []byte) ([]byte, error)

	// Encrypts and signs the message with the group's key.
	Encrypt(cancel <-chan struct{}, token Token, cipher SymCipher, msg []byte) ([]byte, error)

	// Verifies the contents using the group's key.
	Decrypt(cancel <-chan struct{}, token Token, c []byte) ([]byte, error)
}

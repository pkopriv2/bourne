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

// func Connect() (Connection, error) {
// return nil, nil
// }

// A signer contains the knowledge necessary to digitally sign messages.
type Signer interface {
	Public() PublicKey
	Sign(rand io.Reader, hsh Hash, msg []byte) ([]byte, error)
}

// Public keys are the basis of identity within the trust ecosystem.  In plain english,
// I don't trust your identity, I only trust your keys.  Therefore, risk planning starts
// with limiting the exposure of your trusted keys.  The more trusted a key, the greater
// the implications of a compromised one.
//
type PublicKey interface {
	Algorithm() KeyAlgorithm
	Verify(hsh Hash, msg []byte, sig []byte) error
	Bytes() []byte
}

// Private keys represent proof of identity and form the basis of how authentication
// confidentiality and integrity concerns are managed within the ecosystem.  Warden
// goes to great lengths to ensure that your data is *NEVER* derivable by any other
// actors (malicious or otherwise), including the system itself.
//
// For high-security private keys, only a signer is required to authenticate with the
// system.  It is never necessary for private key to leave the local system, which is
// convienent in enviroments where access to the private key is not possible
// (e.g. embedded hardware security modules).
//
// For user-defined keys, users may wish to store encrypted backups of their private
// keys.  The plaintext version of these keys (PrivateKey#Bytes()) never leave the
// the local machine, ensuring that the trust ecosystem has no knowledge of the key
// values.  Consumers may access their keys using
//
type PrivateKey interface {
	Signer
	Algorithm() KeyAlgorithm
	Bytes() []byte
}

// An access pad gives access to the various authentication methods.
//
// # Authentication
//
// Authentication within the trust ecosystem is performed using a hybrid signature
// + Leslie-Lamport's One-Time-Pass scheme.  In other words, the signature
//
type AccessPad interface {

	// Authenticates using a simple signature scheme. The signer will be asked
	// to sign a simple message, which will then be used as proof that the caller
	// has direct signing access to the private key component of the public key.
	//
	// Note: The public key is only used as a means of performing a simple account
	// lookup and is not used to verify the signature.  The signature verification
	// happens
	//
	WithSignature(Signer) (Token, error)

	// Authenticates with a simple user-generated pin.
	WithPin(user string, pin []byte) (Session, error)

	// // Authenticates using the default username/password combo.  We currently
	// // do not support multifactor auth, so this should be considered the weakest
	// // of the 3, but does not require that the user has direct access to his/her
	// // private key.
	WithPassword(user string, pass []byte)
}

// A token represents an authenticated session with the trust ecosystem.  Tokens
// contain a signed message from the trust service - plus a hashed form of the
// authentication credentials.
//
type Token interface {

	// Returns the public key associated with the token.
	PublicKey(cancel <-chan struct{}) PublicKey

	// Reconstructs the secret key from the token details.  The reconstruction process is such
	// that only the local process has enough knowledge to reconstruct it.  Moreover, the
	// reconstruction process is collaborative, meaning it requires elements from both the
	// user and the trust system.
	//
	// The private key will be promptly destroyed once the given closure returns.
	SecretKey(cancel <-chan struct{}, fn func([]byte)) error
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

// // Group levels set the basic policy for members of a trusted group to make changes to the group.
// type TrustLevel int
//
// const (
//
// // Minimum trust allows members to view the shared secret, while disallowing membership changes
// Minimum TrustLevel = 0
//
// // Maximum trust allows members to invite and evict other members.
// Maximum TrustLevel = math.MaxUint32
// )
//
// // A trust is a group of members that maintain a shared secret.
// type Group interface {
//
// // Authenticates the user with the trust, returning an access token for the trust
// Auth(cancel <-chan struct{}, session Session) (Token, error)
//
// // List all members of the trust.
// Members(cancel <-chan struct{}, token Token) []uuid.UUID
//
// // List all pending invitations of the trust.
// Invitations(cancel <-chan struct{}, token Token) []Invitation
//
// // Generates an invitation for the given member at the specified trust level
// Invite(cancel <-chan struct{}, token Token, member uuid.UUID, lvl TrustLevel) (Invitation, error)
//
// // Evicts a member from the group.  Only those with maximum trust may evict members
// Evict(cancel <-chan struct{}, token Token, member uuid.UUID) (Invitation, error)
//
// // Accesses the underlying secret.
// Secret(cancel <-chan struct{}, token Token, fn func(secret []byte))
// }
//
// // An invitation is a cryptographically secured message that may only be accepted
// // by the intended recipient.  These technically can be shared publicly, but exposure
// // should be limited (typically only the trust system needs to know)
// type Invitation struct {
// Id        uuid.UUID
// Recipient uuid.UUID
// Created   time.Time
// Expires   time.Time
// }

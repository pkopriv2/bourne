package warden

import (
	"errors"
	"math"
	"time"

	uuid "github.com/satori/go.uuid"
)

// Common errors
var (
	SessionExpiredError = errors.New("Warden:ExpiredSession")
	SessionInvalidError = errors.New("Warden:InvalidSession")
	TokenExpiredError   = errors.New("Warden:ExpiredToken")
	TokenInvalidError   = errors.New("Warden:InvalidToken")
)

// A session is a special system-level token that authenticates an individual's membership within
// the trust ecosystem.  They are identical, in spirit, to a JSON Web Token (JWT) albeit with a
// hardened set of required "claim" fields.
//
type Session struct {
	IssuedTo  uuid.UUID
	IssuedBy  uuid.UUID
	ValidBeg  time.Time
	ValidEnd  time.Time
	Signature Signature
}

// A token is a data structure that may be used to access a secure object.
//
// A token consists of 3 parts:
//
// 1. A session,
// 2. A credential,
// 3. A signature
//
// Tokens contain a credential that has been securely hashed using a random
// salt owned by the resource.  The resulting hash should be considered
// resistant to most reversal attacks (rainbow tables, brute-force, etc...)
//
// Moreover, the hashed credential is NOT immediately valuable on its own. In order
// to be used, it must be paired with another random salt in order to access the secure
// resource.  This second round of hashing ensures that even if the value of the token
// is leaked beyond the process boundaries, it cannot be used to access the
// associated resource.
//
// Lastly, the token contains a signature which was signed with a key specific to
// the instance of the object being accessed.  Tokens, therefore, cannot be distributed
// or shared in any way.
type Token struct {
	Session   Session
	payload   []byte
	signature []byte
}

// A secret is a durable, cryptographically secure structure that protects a long-lived secret.
//
// The plaintext secret never leaves the local process space and any elements that are durably
// stored or distributed are encrypted in such a way that only the owner of the secret
// has enough knowledge to rederive it.
//
// Local copies of the secret (even in encrypted form) should be discarded as soon as
// the use for the secret has passed.
type Secret interface {

	// Authenticates the user (if the supplied access code name and value match) and
	// generates an access token that may be used to access the secret.
	Auth(cancel <-chan struct{}, session Session, name string, code []byte) (Token, error)

	// Returns a list of all the access codes that may be used to access this secret.
	//
	// An access code name is a commonly known login method (e.g. PIN, PASS, KEY, etc..)
	AccessCodes(cancel <-chan struct{}) []string

	// Sets the access code of the given name to the given value.  If the access code already
	// exists, this has the effect of renewing the code.
	//
	// The access code will expire at roughly time.Now().After(expire).  Once expired, the
	// code may only be used to issue tokens for the purposes of renewing the code.
	SetAccessCode(cancel <-chan struct{}, token Token, name string, code []byte) error

	// Permanently removes an access code from the secret.  If the access code in question
	// is the only remaining code, the secret will no longer be accessible.
	//
	// Use with caution.  This operation CANNOT BE UNDONE.
	DelAccessCode(cancel <-chan struct{}, token Token, name string) error

	// Applies the given closure to the plaintext secret.
	//
	// Extreme care should be taken NOT to leak the secret beyond the boundaries of the
	// closure.  By convention, the secret is promptly destroyed once the closure returns
	// and the plaintext secret is never distributed to any 3rd party systems.
	Use(cancel <-chan struct{}, token Token, fn func([]byte)) error
}

// Trust levels set the basic policy for members of a trusted group to make changes to the group.
type TrustLevel int

const (

	// Minimum trust allows members to view the shared secret, while disallowing membership changes
	Minimum TrustLevel = 0

	// Maximum trust allows members to invite and evict other members.
	Maximum TrustLevel = math.MaxUint32
)

// A trust is a group of members that maintain a shared secret.
type Trust interface {

	// Authenticates the user with the trust, returning an access token
	Auth(cancel <-chan struct{}, session Session, code string, pass []byte) (Token, error)

	// List all members of the trust.
	Members(cancel <-chan struct{}, token Token) []uuid.UUID

	// List all pending of the trust.
	Invites(cancel <-chan struct{}, token Token) []Invitation

	// Accepts an invitation.  The invitation *MUST* be addressed to the user of token.
	Accept(cancel <-chan struct{}, token Token, invitation uuid.UUID) error

	// Generates an invitation for the given member at the specified trust level
	Invite(cancel <-chan struct{}, token Token, member uuid.UUID, lvl TrustLevel) (Invitation, error)

	// Evicts a member from the group.  Only those with maximum trust may evict members
	Evict(cancel <-chan struct{}, token Token, member uuid.UUID) (Invitation, error)

	// Accesses the underlying secret.
	Use(cancel <-chan struct{}, token Token, fn func(secret []byte))
}

// An invitation is a cryptographically secured message that may only be accepted
// by the intended recipient.  These technically can be shared publicly, but exposure
// should be limited (typically only the trust system needs to know)
type Invitation struct {
	Id        uuid.UUID
	Recipient uuid.UUID
	Created   time.Time
	Expires   time.Time
}

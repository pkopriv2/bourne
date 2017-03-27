package warden

import (
	"crypto"
	"io"
	"time"

	uuid "github.com/satori/go.uuid"
)

// TODO: Consider exposing JWT directly for session management as they are going to be the primary
// TODO: carrier mechanism anyway and look decently similar.
// TODO: I think the only custom component is going to be the Token.

// A general purpose access code.  Access codes are short-term passphrases that may
// be used to open a lock.
type Code interface {

	// The common name of the access code (e.g. PIN,KEY,PASS,etc...)
	Name() string

	// Generates a token, which may be used to unlock the associated resource.
	access(session Session, key []byte, pass []byte) (Token, error)
}

// A lock is a durable, cryptographically secure structure that protects a long-lived secret.
type Lock interface {

	// Returns information on all available access codes.
	Codes() []Code

	// Generates an access token for the lock.  The tokane may only be used with this lock.
	Open(session Session, code string, pass []byte) (Token, error)

	// Sets the access code on the lock.  Renews if the code with the given name already exists.
	SetCode(token Token, code string, pass []byte, expire time.Duration) error

	// Removes an access code from the lock.
	DelCode(token Token, code string, pass []byte, expire time.Duration) error

	// Accesses the underlying secret.  The secret is promply destroyed once the closure returns.
	open(token Token) ([]byte, error)
}

// A member is the basis of identity within the trust ecosystem.
type Member interface {

	// The id of the member (must be universally unique)
	Id() uuid.UUID

	// Returns the lock associated with this member.  Tokens generated from this lock may
	// be used to access the secure components of the member (i.e. the private key)
	Lock() (Lock, error)

	// Returns the public key component of the pair
	PublicKey() crypto.PublicKey

	// Returns the private key, decrypting it with
	PrivateKey(Token, func(crypto.PrivateKey)) error
}

// This represents the basic abstraction for establishing trust within
// a group, while denying trust from outside the group.  At its basis, a
// group is simply a collection of members and the group protects a
// key pair.
//
// The security axioms:
//
//  * Different groups DO NOT trust each other
//
// 	* No groups fully trust the system. (i.e. the system must never be able
// 	  to derive any group's secret, unless the system has been invited)
//
// 	* In order to join a group, a member must be invited by another member.
//
// 	* Once a group no longer has any members, its secret is lost forever!
//
// 	* Members may leave the group - either voluntarily or forced.
//
type Trust interface {

	// Generates a token for use in accessing the trust.
	Open(session Session, code string, pass []byte) (Token, error)

	// Accepts an invitation.  The invitation *MUST* be be addressed to the user of session.
	Accept(session Session, invitation Invitation, code string, pass []byte) error

	// Generates an invitation for the member.
	Invite(token Token, memberId uuid.UUID) (Invitation, error)

	// Returns the document with the given id.
	GetDocument(token Token, id uuid.UUID) (io.Reader, error)

	// Returns the document with the given id
	PutDocument(token Token, id uuid.UUID, data io.Reader) error

	// Deletes the document with the given id.
	DelDocument(token Token, id uuid.UUID) error
}

// An invitation is a cryptographically secured message that may only be accepted
// by the intended recipient.  These technically can be shared publicly.
type Invitation struct {
	Id       uuid.UUID
	TrustId  uuid.UUID
	MemberId uuid.UUID
	msg      asymCipherText
}

func (i Invitation) decryptKey(memberKey crypto.PrivateKey) ([]byte, error) {
	return nil, nil
}

func (i Invitation) decryptMsg(key []byte) ([]byte, error) {
	return nil, nil
}

func (i Invitation) Decrypt(memberKey crypto.PrivateKey) ([]byte, error) {
	return nil, nil
}

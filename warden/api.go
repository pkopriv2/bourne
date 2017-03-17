package warden

import (
	"crypto"
	"time"

	uuid "github.com/satori/go.uuid"
)

// A token is a cryptographically secure
type Token struct {
	TTL    int
	Claims map[string]string
	Auth   []byte
}

func (t Token) Encrypt(key crypto.PublicKey) ([]byte, error) {
	return nil, nil
}

func (t Token) Authenticate(key crypto.PrivateKey) error {
	return nil
}

func DecryptToken(token []byte, key crypto.PrivateKey) (Token, error) {
	return Token{}, nil
}

// Publicly viewable information of the challenge
type AccessCodeInfo struct {
	Id       uuid.UUID
	Name     string
	Ttl      int
	Used     int
	Created  time.Time
}

// The unsecured contents of the safe.
type SafeContents interface {

	// The secured secret (Extreme care should be taken NOT to leak this information beyond what's required.)
	Secret() []byte

	// Returns basic info of all currently active access codes.
	AccessCodes() []AccessCodeInfo

	// Adds an authorized access code to the safe.
	AddAccessCode(name string, code []byte, ttl int) error

	// Deletes an authorized challenge from the safe box.
	DelAccessCode(name string) error
}

// A safe is a durable, cryptographically secure structure that protects a secret key.
type Safe interface {
	Id() uuid.UUID

	// The safe box's public key
	PublicKey() crypto.PublicKey

	// Returns all the challenges
	AccessCodes() []AccessCodeInfo

	// Opens the box using the given challenge.  The provided closure will give raw access
	// to the protected resources.  Consumers should be careful NOT to allow elements of the
	// safe room to be leaked to the external environment.  Once the given closure returns
	// the core secret of the box is promptly discarded.
	Open(challengeName string, challengeSecret []byte, fn func(s SafeContents)) (err error)
}

// A member represents the basis of identity within the trust ecosystem.
type Member interface {

	// Returns the member's id.
	Id() uuid.UUID

	// Returns the member's personal safe.
	Safe() (Safe, error)
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

type TrustContents interface {



	// Generates a new invitation.  The returned invitation is cryptographically
	// secure and may be shared publicly.  However, it should be limited to
	InviteMember(memberId uuid.UUID) (Invitation, error)

	// Evicts the
	EvictMember(memberId uuid.UUID)
}

type Trust interface {

	// Opens the trust using
	OpenTrust(self Member, codeName string, code []byte, fn func(t Trust)) error

	// Accepts an invitation using the target entity's private key.
	Accept(invitation Invitation) error
}

// An invitation is a cryptographically secured message that may only be accepted
// by the intended recipient.  These technically can be shared publicly.
type Invitation struct {
	Id           uuid.UUID
	TrustId      uuid.UUID
	EntityId     uuid.UUID
	encryptedKey []byte
	encryptedMsg []byte
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

//
// type SecureDocument interface {
// Id() uuid.UUID
// KeyRing() KeyRing
// Raw() (io.Reader, error)
// Decrypt(key []byte) (io.Reader, error)
// }
//
// type TrustStore interface {
//
// // Returns the document
// GetDocument(uuid.UUID) (SecureDocument, error)
//
// // Stores the secure document stream.
// StoreDocument(uuid.UUID, io.Reader) (SecureDocument, error)
// }
//
// type Client interface {
// }

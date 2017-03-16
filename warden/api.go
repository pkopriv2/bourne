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
type ChallengeInfo struct {
	Id       uuid.UUID
	Name     string
	Ttl      int
	Used     int
	Created  time.Time
	Accessed time.Time
}

// The safe room allows direct access to the secrets of the corresponding safe box.  And allows
// changes.
type SafeRoom interface {

	// The secured secret
	Secret() []byte

	// Returns the private key of the box.
	PrivateKey() crypto.PrivateKey

	// Returns all the currently active challenges.
	AllChallenges() map[string]ChallengeInfo

	// Adds an authorized challenge to the safe box.
	AddChallenge(name string, secret []byte, ttl int) error

	// Deletes an authorized challenge from the safe box.
	DelChallenge(name string) error
}

// A key safe is a durable, cryptographically secure structure that protects a secret key.
type SafeBox interface {
	Id() uuid.UUID

	// The safe box's public key
	PublicKey() crypto.PublicKey

	// Returns all the challenges
	Challenges() map[string]ChallengeInfo

	// Opens the box using the given challenge.  The provided closure will give raw access
	// to the protected resources.  Consumers should be careful NOT to allow elements of the
	// safe room to be leaked to the external environment.  Once the given closure returns
	// the core secret of the box is kkkkkk
	Open(challengeName string, challengeSecret []byte, fn func(s SafeRoom)) (err error)
}

// A member represents the basis of identity within the trust ecosystem.
type Member interface {

	// Returns the member's id.
	Id() uuid.UUID

	// Returns the member's key ring.
	SafeBox() (SafeBox, error)
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

	// Generates a new invitation.  The returned invitation is cryptographically
	// secure and may be shared publicly.  However, it should be limited to
	Invite(grantor Member, id uuid.UUID, secret []byte, key crypto.PublicKey) (Invitation, error)

	// Evicts the
	Evict(memberId uuid.UUID)
}

type Group interface {

	// Opens the trust using
	OpenTrust(self Member, challengeName string, challengeBytes []byte, fn func(t Trust)) error

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

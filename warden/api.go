package warden

import (
	"crypto"

	uuid "github.com/satori/go.uuid"
)

// A secret contains an actual secret.  These should be handled with extreme care.
type Secret struct {
	Id    uuid.UUID
	Bytes []byte
}

// A key ring represents a protected public/private key pair.
type KeyRing interface {

	// Return the public key component of this key ring.  This may be shared publicly
	// without compromising the security of the ring.
	PublicKey() (crypto.PublicKey, error)

	// Return the private key component of this key ring.  This should be used and discarded
	PrivateKey(Secret) (crypto.PrivateKey, error)

	// Adds a secret to the key ring.  Caller must supply the actual private key
	// in order to add a secret.
	AddSecret(crypto.PrivateKey, Secret) error

	// Adds a secret to the key ring.  Caller must supply the actual private key
	// in order to add a secret.
	DelSecret(crypto.PrivateKey, Secret) error
}

// A member represents the basis of identity within the trust ecosystem.
type Member interface {

	// Returns the member's id.
	Id() uuid.UUID

	// Returns the member's key ring.
	KeyRing() (KeyRing, error)
}

// This represents the basic abstraction for establishing trust within
// a group, while denying trust from outside the group.  At its basis, a
// group is simply a collection of members and the group protects a
// single secret (a public/private key pair).
//
// Membership of the group must be allowed to increase and decrease over
// time, while retaining the security of the group.  The secret may be
// obtained by a trusted member of the group, but the knowledge of how
// the secret is obtained is maintained by the system (and is never exposed
// to the group members).  Most importantly, the system itself CANNOT obtain
// the secret.
//
// Individual members of a group may be compromised.  Nothing can be done
// while the compromise is taking place, but once it has been discovered,
// the group must be able to revoke trust from the member, while retaining
// trust within the rest of the group.
//
// If trust is removed from a member, the remaining members of the group
// must be allowed to work together to re-establish trust.  Otherwise, they
// may choose to abandon the entire secret.  This is akin to abandoning
// the entire group.
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
type Group interface {
	Key() KeyRing

	// Generates a new invitation.  The returned invitation is cryptographically
	// secure and may be shared publicly.  However, it should be limited to
	Invite(uuid.UUID, crypto.PublicKey) (Invitation, error)

	// Accepts an invitation using the target entity's private key.
	Accept(Invitation, crypto.PrivateKey) error

	// Removes a member from the group.
	Evict(uuid.UUID) error
}

// An invitation is a cryptographically secured message that may only be accepted
// by the intended recipient.
type Invitation struct {
	Id       uuid.UUID
	TrustId  uuid.UUID
	EntityId uuid.UUID
	_raw_    []byte
}

func (i Invitation) Decrypt(crypto.PrivateKey) ([]byte, error) {
	return nil, nil
}

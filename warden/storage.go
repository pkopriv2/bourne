package warden

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

// Basic errors
var (
	StorageError          = errors.New("Warden:StorageError")
	StorageInvariantError = errors.Wrap(StorageError, "Warden:StorageError")
)

type storage interface {

	// Stores the subscriber and the default member code.  These elements
	// of a subscriber are guaranteed to be static throughout its lifetime.
	//
	// Returns an error if the subscriber already exists.
	SaveMember(Member, MemberCode) error

	// Member
	LoadMemberByLookup([]byte) (Member, MemberCode, bool, error)

	// Loads the subscriber, returning true if it existed.
	LoadMemberById(uuid.UUID) (Member, bool, error)

	// Saves the trust, and the issuer's code + cert.  Must all be done in same transaction
	SaveTrust(TrustCore, TrustCode, SignedCertificate) error

	// Loads the given trust.
	LoadTrustCore(uuid.UUID) (TrustCore, bool, error)

	// Loads the given trust.
	LoadTrustCode(trustId, memberId uuid.UUID) (TrustCode, bool, error)

	// Loads the certificate
	LoadCertificate(uuid.UUID) (SignedCertificate, bool, error)

	// Loads the certificate
	LoadActiveCertificate(memberId, trustId uuid.UUID) (SignedCertificate, bool, error)

	// Saves the trust, and the issuer's code + cert.  Must all be done in same transaction
	SaveInvitation(Invitation) error

	// Loads the invitation.
	LoadInvitationById(uuid.UUID) (Invitation, bool, error)

	// Loads the invitation.
	LoadInvitationsByMember(uuid.UUID, int,int) ([]Invitation, error)

	// Saves the certificate
	SaveCertificate(s SignedCertificate, code TrustCode) error

	// Revokes the certificate
	RevokeCertificate(trusteeId, trustId uuid.UUID) error
}

func EnsureMember(store storage, id uuid.UUID) (Member, error) {
	s, o, e := store.LoadMemberById(id)
	if e != nil || !o {
		return s, common.Or(e, errors.Wrapf(StorageInvariantError, "No member [%v]", id))
	}
	return s, nil
}

func EnsureTrust(store storage, id uuid.UUID) (TrustCore, error) {
	s, o, e := store.LoadTrustCore(id)
	if e != nil || !o {
		return s, common.Or(e, errors.Wrapf(StorageInvariantError, "No trust [%v]", id))
	}
	return s, nil
}

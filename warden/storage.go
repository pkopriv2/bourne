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

	// Stores the subscriber and the default authenticator.  These elements
	// of a subscriber are guaranteed to be static throughout its lifetime.
	//
	// Returns an error if the subscriber already exists.
	SaveMember(sub Membership, auth AccessShard) (Member, AccessCode, error)

	// Member
	LoadMemberByLookup(lookup []byte) (Member, AccessCode, bool, error)

	// Loads the subscriber, returning true if it existed.
	LoadMemberById(id uuid.UUID) (Member, bool, error)

	// // Loads the subscriber, returning true if it existed.
	// LoadMemberByKey(key string) (Member, bool, error)

	// // Stores an auxiliary encrypted key pair.
	// SaveAuxKey(sub,alias string, pair SignedKeyPair) error
	//
	// // Loads an auxiliary key
	// LoadAuxKey(id string) (SignedKeyPair, error)
	//
	// // Loads an auxiliary key.
	// LoadAuxKeyId(sub,key string) (string, error)

	// // // Loads a specific version of an auxillary key.
	// LoadLatestMemberAuxKey(sub, name string) (int, bool, error)

	// SaveMemberAuth(subscriber, method string, auth SignedOracleKey) error
	// LoadMemberAuth(subscriber, method string) (StoredAuthenticator, bool, error)

	// LoadTrust(id string) (storedTrust, bool, error)
	// SaveTrust(dom Trust) error
	// LoadCertificate(id uuid.UUID) (SignedCertificate, error)
}

func EnsureMember(store storage, id uuid.UUID) (Member, error) {
	s, o, e := store.LoadMemberById(id)
	if e != nil || !o {
		return s, common.Or(e, errors.Wrapf(StorageInvariantError, "No member [%v]", id))
	}
	return s, nil
}

// func EnsureMemberAuth(store Storage, id, method string) (StoredAuthenticator, error) {
// s, o, e := store.LoadMemberAuth(id, method)
// if e != nil || !o {
// return s, common.Or(e, errors.Wrapf(StorageInvariantError, "No subscriber auth [%v,%v]", id, method))
// }
// return s, nil
// }

// func ensureTrust(store storage, id string) (storedTrust, error) {
// d, o, e := store.LoadTrust(id)
// if e != nil || !o {
// return d, common.Or(e, errors.Wrapf(StorageInvariantError, "Trust not found [%v]", id))
// }
// return d, nil
// }

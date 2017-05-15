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

type storedSubscriber struct {
	Subscriber
}

type storedPublicKey struct {
	PublicKey
}

type storedKeyPair struct {
	SignedKeyPair
}

type storedAuthenticator struct {
	SignedEncryptedShard
}

type storedTrust struct {
	Identity KeyPair
	Oracle   SignedShard
}

type storage interface {

	// Stores the subscriber and the default authenticator.  These elements
	// of a subscriber are guaranteed to be static throughout its lifetime.
	//
	// Returns an error if the subscriber already exists or the authenticator
	// has not been properly signed.
	SaveSubscriber(sub Subscriber, auth SignedEncryptedShard) error

	// Loads the subscriber, returning true if it existed.
	LoadSubscriberById(id uuid.UUID) (Subscriber, bool, error)

	// // Loads the subscriber, returning true if it existed.
	// LoadSubscriberByKey(key string) (Subscriber, bool, error)

	// // Stores an auxiliary encrypted key pair.
	// SaveAuxKey(sub,alias string, pair SignedKeyPair) error
	//
	// // Loads an auxiliary key
	// LoadAuxKey(id string) (SignedKeyPair, error)
	//
	// // Loads an auxiliary key.
	// LoadAuxKeyId(sub,key string) (string, error)

	// // // Loads a specific version of an auxillary key.
	// LoadLatestSubscriberAuxKey(sub, name string) (int, bool, error)

	// SaveSubscriberAuth(subscriber, method string, auth SignedOracleKey) error
	// LoadSubscriberAuth(subscriber, method string) (StoredAuthenticator, bool, error)

	LoadTrust(id string) (storedTrust, bool, error)
	SaveTrust(dom Trust) error

	// LoadCertificate(id uuid.UUID) (SignedCertificate, error)
}

// func EnsureSubscriber(store Storage, key string) (StoredSubscriber, error) {
// s, o, e := store.LoadSubscriberIdByKey(key)
// if e != nil || !o {
// return s, common.Or(e, errors.Wrapf(StorageInvariantError, "No subscriber [%v]", sub))
// }
// return s, nil
// }

// func EnsureSubscriberAuth(store Storage, id, method string) (StoredAuthenticator, error) {
// s, o, e := store.LoadSubscriberAuth(id, method)
// if e != nil || !o {
// return s, common.Or(e, errors.Wrapf(StorageInvariantError, "No subscriber auth [%v,%v]", id, method))
// }
// return s, nil
// }

func ensureTrust(store storage, id string) (storedTrust, error) {
	d, o, e := store.LoadTrust(id)
	if e != nil || !o {
		return d, common.Or(e, errors.Wrapf(StorageInvariantError, "Trust not found [%v]", id))
	}
	return d, nil
}

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

type StoredSubscriber struct {
	Subscriber
}

type StoredPublicKey struct {
	PublicKey
}

type StoredKeyPair struct {
	SignedKeyPair
}

type StoredAuthenticator struct {
	SignedOracleKey
}

type StoredDomain struct {
	Identity KeyPair
	Oracle   SignedOracle
}

type Storage interface {

	// Stores the subscriber and the default authenticator.  These elements
	// of a subscriber are guaranteed to be static throughout its lifetime.
	//
	// Returns an error if the subscriber already exists or the authenticator
	// has not been properly signed.
	SaveSubscriber(sub Subscriber, auth SignedOracleKey) error

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

	LoadDomain(id string) (StoredDomain, bool, error)
	SaveDomain(dom Domain) error

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

func EnsureDomain(store Storage, id string) (StoredDomain, error) {
	d, o, e := store.LoadDomain(id)
	if e != nil || !o {
		return d, common.Or(e, errors.Wrapf(StorageInvariantError, "Domain not found [%v]", id))
	}
	return d, nil
}
